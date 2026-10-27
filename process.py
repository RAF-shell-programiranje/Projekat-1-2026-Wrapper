#!/usr/bin/env python3
"""Aggregate one chunk of DataPipe input by group.

The interface of this program (arguments, output format, stderr lines, exit
codes, signals and the PROC_* environment variables) is the contract described
in the project text, section "Dati programi". Do not modify this file: grading
uses the original copy.
"""

import argparse
import fcntl
import math
import os
import random
import re
import signal
import sys
import time
import types

INPUT_HEADER = "transaction_id,timestamp,region,category,amount,quantity"
OUTPUT_HEADER = "group,count,sum_amount,min_amount,max_amount,sum_quantity"
GROUP_COLUMNS = {"region": 2, "category": 3}

PROGRESS_EVERY = 5000  # rows between PROGRESS lines,
PROGRESS_GAP = 0.5     # but at most this many seconds between them while working
SLOW_EVERY = 1000      # PROC_SLOW_MS applies after every this many rows
SLEEP_SLICE = 0.2      # longest single sleep, so signals and PROGRESS stay prompt
MAX_ROW_WARNINGS = 20  # WARN skipped_row lines per chunk

# Faults drawn at random from the PROC_*_RATE probabilities, in this order.
RANDOM_FAULTS = [
    ("fail", "PROC_FAIL_RATE"),
    ("hang", "PROC_HANG_RATE"),
    ("hardhang", "PROC_HARDHANG_RATE"),
    ("crash", "PROC_CRASH_RATE"),
    ("corrupt", "PROC_CORRUPT_RATE"),
]
KNOWN_FAULTS = {name for name, _ in RANDOM_FAULTS}
MIDWAY_FAULTS = ("fail", "hang", "hardhang", "crash")  # strike at the fault point


# ---- KEEP IN SYNC: identical in gen_data.py and process.py -------------------
_ID = re.compile(r"[0-9]+")
_TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}")
_AMOUNT = re.compile(r"[0-9]+\.[0-9]{2}")
_QUANTITY = re.compile(r"[1-9][0-9]*")


def row_problem(fields):
    """Return None if the split row is valid, otherwise the reason to skip it."""
    if len(fields) != 6:
        return "bad_fields"
    if not _ID.fullmatch(fields[0]) or not fields[0].strip("0"):
        return "bad_id"
    if not _TIMESTAMP.fullmatch(fields[1]):
        return "bad_timestamp"
    if not fields[2] or not fields[3]:
        return "bad_group"
    if not _AMOUNT.fullmatch(fields[4]):
        return "bad_amount"
    if not _QUANTITY.fullmatch(fields[5]):
        return "bad_quantity"
    return None


def to_cents(amount):
    """'1299.99' -> 129999, for an amount that passed row_problem()."""
    return int(amount.replace(".", ""))


def fmt_cents(cents):
    """129999 -> '1299.99'"""
    return f"{cents // 100}.{cents % 100:02d}"
# ---- END KEEP IN SYNC --------------------------------------------------------


class State:
    """What signal handlers and exit paths need to know about this process."""
    out_path = None  # OUT from the moment we start writing it; removed on SIGTERM/SIGINT
    audit_id = None  # "<pid> <basename>" once the audit START line is written


def emit(line):
    """Write one line to stderr right away, in a single write()."""
    try:
        os.write(2, (line + "\n").encode("utf-8", "backslashreplace"))
    except OSError:
        pass


def die(message, code):
    emit("ERROR " + message)
    sys.exit(code)


def audit(text):
    """Append one line to PROC_AUDIT_LOG, if it is set (the grader reads it)."""
    path = os.environ.get("PROC_AUDIT_LOG")
    if not path:
        return
    line = f"{int(time.time() * 1000)} {text}\n".encode("utf-8", "backslashreplace")
    try:
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o666)
        try:
            os.write(fd, line)
        finally:
            os.close(fd)
    except OSError:
        pass


def audit_end(code):
    if State.audit_id is not None:
        audit(f"END {State.audit_id} {code}")
        State.audit_id = None


def on_signal(signum, frame):
    """SIGTERM/SIGINT: remove the unfinished OUT and exit with 128 + signal."""
    if State.out_path is not None:
        try:
            os.unlink(State.out_path)
        except OSError:
            pass
    emit("ERROR terminated" if signum == signal.SIGTERM else "ERROR interrupted")
    audit_end(128 + signum)
    os._exit(128 + signum)


def install_signal_handlers():
    signal.signal(signal.SIGTERM, on_signal)
    # Like any other program, keep SIGINT ignored if it was ignored on entry:
    # bash starts the background jobs of a script that way, so Ctrl+C reaches
    # only the script, which then stops its workers itself.
    if signal.getsignal(signal.SIGINT) != signal.SIG_IGN:
        signal.signal(signal.SIGINT, on_signal)


class ArgumentParser(argparse.ArgumentParser):
    """argparse whose errors also end with an 'ERROR <message>' line (exit 2)."""

    def error(self, message):
        self.print_usage(sys.stderr)
        sys.stderr.flush()
        die(f"bad arguments: {message}", 2)


def parse_args(argv):
    p = ArgumentParser(
        prog="process.py", allow_abbrev=False,
        description="Aggregate one chunk of DataPipe input by group.")
    p.add_argument("--input", required=True, metavar="CHUNK",
                   help="input chunk: CSV with the DataPipe header")
    p.add_argument("--group-by", choices=("region", "category"), default="region",
                   help="grouping column (default: region)")
    p.add_argument("--output", required=True, metavar="OUT",
                   help="result file, written in place (not atomically)")
    return p.parse_args(argv)


def env_number(name, default, upper=None):
    """A non-negative number from the environment; unset or empty gives default."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        value = math.nan
    if not (value >= 0 and math.isfinite(value) and (upper is None or value <= upper)):
        die(f"bad {name} value: {raw}", 2)
    return value


def load_plan(path):
    """PROC_FAULT_PLAN: lines '<basename> <attempt> <fault>'; '#' starts a comment."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
    except OSError as e:
        die(f"cannot read PROC_FAULT_PLAN {path}: {e.strerror}", 2)
    plan = {}
    for number, line in enumerate(lines, 1):
        parts = line.split("#", 1)[0].split()
        if not parts:
            continue
        if len(parts) != 3 or not re.fullmatch(r"[1-9][0-9]*", parts[1]):
            die(f"bad PROC_FAULT_PLAN line {number}: {line.strip()}", 2)
        plan[(parts[0], int(parts[1]))] = parts[2]
    return plan


def read_env():
    env = types.SimpleNamespace()
    env.rates = [(name, env_number(var, 0.0, upper=1.0)) for name, var in RANDOM_FAULTS]
    env.slow = env_number("PROC_SLOW_MS", 0.0) / 1000.0  # in seconds
    env.seed = os.environ.get("PROC_SEED", "")
    plan_path = os.environ.get("PROC_FAULT_PLAN", "")
    env.plan = load_plan(plan_path) if plan_path else {}
    return env


def bump_attempt_counter(basename):
    """Count this run as the next attempt for this input name, in PROC_STATE_DIR."""
    state_dir = os.environ.get("PROC_STATE_DIR") or os.path.join(
        os.environ.get("TMPDIR") or "/tmp", "process_py_state")
    try:
        os.makedirs(state_dir, exist_ok=True)
        with open(os.path.join(state_dir, basename + ".attempts"), "a+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.seek(0)
            try:
                attempt = int(f.read()) + 1
            except ValueError:
                attempt = 1
            f.seek(0)
            f.truncate()
            f.write(str(attempt))
    except OSError as e:
        die(f"cannot update the attempt counter in {state_dir}: {e.strerror}", 2)
    return attempt


def pick_fault(rng, env, basename, attempt):
    """The fault for this attempt: the plan entry if there is one, else a random draw."""
    draw = rng.random()  # drawn either way, so the plan does not shift the rng sequence
    planned = env.plan.get((basename, attempt))
    if planned is not None:
        return planned
    total = 0.0
    for name, rate in env.rates:
        total += rate
        if draw < total:
            return name
    return None


class Progress:
    """PROGRESS lines: every PROGRESS_EVERY rows, and at least every PROGRESS_GAP s."""

    def __init__(self):
        self.last = time.monotonic()

    def report(self, rows):
        emit(f"PROGRESS rows={rows}")
        self.last = time.monotonic()

    def report_if_due(self, rows):
        if time.monotonic() - self.last >= PROGRESS_GAP:
            self.report(rows)


def count_lines(path, progress):
    """Number of lines in the file, counted fast (the fault point depends on it)."""
    lines = 0
    last = b"\n"
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            lines += block.count(b"\n")
            last = block[-1:]
            progress.report_if_due(0)
    if last != b"\n":
        lines += 1  # a last line without a newline
    return lines


def slow_down(seconds, rows, progress):
    """PROC_SLOW_MS: sleep in short slices, still reporting progress on time."""
    deadline = time.monotonic() + seconds
    while True:
        left = deadline - time.monotonic()
        if left <= 0:
            return
        time.sleep(min(left, SLEEP_SLICE))
        progress.report_if_due(rows)


def strike(fault, out):
    """Run a fault that hits in the middle of the chunk."""
    if fault == "fail":
        out.close()  # OUT keeps just the header
        die("simulated failure", 1)
    if fault == "crash":
        out.flush()
        os.kill(os.getpid(), signal.SIGKILL)
    if fault == "hardhang":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    while True:  # hang (SIGTERM still works) or hardhang (only SIGKILL helps)
        time.sleep(SLEEP_SLICE)


def process_rows(src, group_col, slow, progress, fault, fault_point, out):
    """Validate and aggregate the data rows of src, which is past the header."""
    groups = {}  # group -> [count, sum_cents, min_cents, max_cents, sum_quantity]
    rows = skipped = 0
    for line in src:
        rows += 1
        fields = line.rstrip("\n").split(",")
        problem = row_problem(fields)
        if problem is not None:
            skipped += 1
            if skipped <= MAX_ROW_WARNINGS:
                emit(f"WARN skipped_row line={rows + 1} reason={problem}")
            elif skipped == MAX_ROW_WARNINGS + 1:
                emit("WARN more skipped rows suppressed")
        else:
            name = fields[group_col]
            cents = to_cents(fields[4])
            quantity = int(fields[5])
            agg = groups.get(name)
            if agg is None:
                groups[name] = [1, cents, cents, cents, quantity]
            else:
                agg[0] += 1
                agg[1] += cents
                if cents < agg[2]:
                    agg[2] = cents
                if cents > agg[3]:
                    agg[3] = cents
                agg[4] += quantity
        if rows % PROGRESS_EVERY == 0:
            progress.report(rows)
        elif rows % 256 == 0:
            progress.report_if_due(rows)
        if slow and rows % SLOW_EVERY == 0:
            slow_down(slow, rows, progress)
        if rows == fault_point and fault in MIDWAY_FAULTS:
            strike(fault, out)
    if rows < fault_point and fault in MIDWAY_FAULTS:
        strike(fault, out)  # fewer rows than counted, e.g. an empty chunk
    return groups, rows, skipped


def format_group(name, agg):
    count, total, low, high, quantity = agg
    return f"{name},{count},{fmt_cents(total)},{fmt_cents(low)},{fmt_cents(high)},{quantity}"


def finish_body(group_lines, rows, skipped, fault, rng):
    """The group rows plus the #END line; a 'corrupt' fault spoils the ending."""
    rows_in = rows
    if fault == "corrupt":
        if rng.random() < 0.5:
            return group_lines  # no #END at all
        rows_in = rows + rng.randint(1, 100)  # an #END that does not match the input
    return group_lines + [f"#END rows_in={rows_in} rows_skipped={skipped} groups={len(group_lines)}"]


def open_output(args):
    """Open OUT. The result is written straight into it, not atomically."""
    State.out_path = args.output
    try:
        return open(args.output, "w", encoding="utf-8", errors="surrogateescape", newline="\n")
    except OSError as e:
        State.out_path = None
        die(f"cannot write output {args.output}: {e.strerror}", 2)


def main(argv):
    started = time.monotonic()
    install_signal_handlers()
    args = parse_args(argv)
    env = read_env()
    basename = os.path.basename(args.input)
    attempt = bump_attempt_counter(basename)
    rng = random.Random(f"{env.seed}:{basename}:{attempt}") if env.seed else random.Random()
    planned = pick_fault(rng, env, basename, attempt)
    fault = planned if planned in KNOWN_FAULTS else None

    pid = os.getpid()
    audit(f"START {pid} {basename} {attempt} {fault or '-'}")
    State.audit_id = f"{pid} {basename}"
    start = f"START input={basename} pid={pid}"
    emit(start)
    if planned is not None and fault is None:
        emit(f"WARN unknown fault name={planned}")

    progress = Progress()
    try:
        data_rows = max(count_lines(args.input, progress) - 1, 0)
        src = open(args.input, encoding="utf-8", errors="surrogateescape", newline="\n")
    except OSError:
        die(f"input not found: {args.input}", 2)
    with src:
        if src.readline().rstrip("\n") != INPUT_HEADER:
            die("bad header", 2)
        fault_point = max(1, int(data_rows * rng.uniform(0.2, 0.8)))
        out = open_output(args)
        out.write(OUTPUT_HEADER + "\n")
        out.flush()  # from here on a partial OUT with just the header exists
        groups, rows, skipped = process_rows(
            src, GROUP_COLUMNS[args.group_by], env.slow, progress, fault, fault_point, out)

    names = sorted(groups)
    rng.shuffle(names)  # the order of groups is deliberately not defined
    lines = finish_body([format_group(name, groups[name]) for name in names],
                        rows, skipped, fault, rng)
    out.write("".join(line + "\n" for line in lines))
    out.close()
    emit(f"DONE rows={rows} skipped={skipped} elapsed={time.monotonic() - started:.2f}")
    return 0


if __name__ == "__main__":
    status = 1
    try:
        status = main(sys.argv[1:])
    except SystemExit as e:
        status = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
    except OSError as e:
        emit(f"ERROR {e}")
    finally:
        audit_end(status)
    sys.exit(status)
