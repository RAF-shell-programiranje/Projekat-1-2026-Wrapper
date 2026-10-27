#!/usr/bin/env python3
"""Generate DataPipe test input and, optionally, the exact expected result.

    python3 gen_data.py --rows N | --size SIZE [--seed S] [--output FILE]
                        [--bad-rate P] [--group-by region|category]
                        [--start-date YYYY-MM-DD] [--expected FILE]

The same seed and options always give a byte-identical file. --expected writes
what a correct pipeline produces for this data: one row per group, sorted by
group in byte order (LC_ALL=C sort), with the invalid rows left out.
"""

import argparse
import datetime
import os
import random
import re
import sys

HEADER = "transaction_id,timestamp,region,category,amount,quantity"
EXPECTED_HEADER = "group,count,sum_amount,avg_amount,min_amount,max_amount,sum_quantity"
BYTES_PER_ROW = 50  # rough average row length, for --size
BATCH_ROWS = 10000  # rows per write
SIZE_UNITS = {"": 1, "k": 1024, "m": 1024 ** 2, "g": 1024 ** 3, "t": 1024 ** 4}

# (value, weight) pairs; a row picks a value with probability weight / sum of weights.
REGIONS = [("Beograd", 30), ("Novi Sad", 20), ("Niš", 15), ("Kragujevac", 15),
           ("Subotica", 10), ("Čačak", 10)]
CATEGORIES = [("elektronika", 1), ("hrana", 1), ("odeća", 1), ("knjige", 1),
              ("sport", 1), ("kuća", 1)]
QUANTITIES = [(1, 30), (2, 20), (3, 14), (4, 10), (5, 8), (6, 6), (7, 5), (8, 3),
              (9, 2), (10, 2)]
# Amount ranges in cents: many cheap purchases, few expensive ones (0.99 - 4999.99).
AMOUNTS = [((99, 4999), 35), ((5000, 19999), 30), ((20000, 99999), 20),
           ((100000, 299999), 10), ((300000, 499999), 5)]


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


def non_negative_int(text):
    if not re.fullmatch(r"[0-9]+", text.strip()):
        raise argparse.ArgumentTypeError(f"not a non-negative integer: {text!r}")
    return int(text)


def size_in_bytes(text):
    m = re.fullmatch(r"([0-9]+)([kmgt]?)(?:i?b)?", text.strip().lower())
    if not m:
        raise argparse.ArgumentTypeError(f"invalid size {text!r} (examples: 500M, 2G)")
    return int(m.group(1)) * SIZE_UNITS[m.group(2)]


def probability(text):
    try:
        value = float(text)
    except ValueError:
        value = -1.0
    if not 0.0 <= value <= 1.0:
        raise argparse.ArgumentTypeError(f"not a number from 0 to 1: {text!r}")
    return value


def iso_date(text):
    try:
        return datetime.date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a YYYY-MM-DD date: {text!r}")


def parse_args(argv):
    p = argparse.ArgumentParser(
        prog="gen_data.py", allow_abbrev=False,
        description="Generate DataPipe test input (CSV) and, optionally, the expected result.")
    amount = p.add_mutually_exclusive_group(required=True)
    amount.add_argument("--rows", type=non_negative_int, metavar="N",
                        help="number of data rows")
    amount.add_argument("--size", type=size_in_bytes, metavar="SIZE",
                        help=f"approximate file size such as 500M or 2G "
                             f"(about {BYTES_PER_ROW} bytes per row)")
    p.add_argument("--seed", type=int, metavar="S",
                   help="random seed; the same seed and options give the same file")
    p.add_argument("--output", metavar="FILE", help="data file (default: stdout)")
    p.add_argument("--bad-rate", type=probability, default=0.0005, metavar="P",
                   help="probability that a row is invalid (default: 0.0005)")
    p.add_argument("--group-by", choices=("region", "category"), default="region",
                   help="grouping used for --expected (default: region)")
    p.add_argument("--start-date", type=iso_date, default="2026-01-01", metavar="YYYY-MM-DD",
                   help="day of the first timestamp (default: 2026-01-01)")
    p.add_argument("--expected", metavar="FILE",
                   help="also write the expected final result for this data to FILE")
    return p.parse_args(argv)


def weighted(pairs):
    """Expand (value, weight) pairs into a list to index with a uniform random number."""
    return [value for value, weight in pairs for _ in range(weight)]


def spoil(fields, rng):
    """Make a row invalid in one of the ways process.py has to notice."""
    kind = int(rng.random() * 6)
    if kind == 0:
        del fields[int(rng.random() * 6)]  # a missing field
    elif kind == 1:
        fields[4] = "N/A"                   # the amount is not a number
    elif kind == 2:
        fields[4] = fields[4][:-1]          # one decimal, e.g. 12.5
    elif kind == 3:
        fields[5] = "0"                     # quantity 0
    elif kind == 4:
        fields[2] = ""                      # empty region
    else:
        fields[1] = "yesterday"             # not a timestamp


def generate(out, rows, args, groups):
    """Write the header and the data rows to out (a binary file). If groups is a
    dict, also add each valid row to it: group -> [count, sum, min, max, quantity]
    (amounts in cents), all in the same pass."""
    rng = random.Random(args.seed)
    rnd = rng.random
    regions, categories = weighted(REGIONS), weighted(CATEGORIES)
    quantities, amounts = weighted(QUANTITIES), weighted(AMOUNTS)
    n_regions, n_categories = len(regions), len(categories)
    n_quantities, n_amounts = len(quantities), len(amounts)
    clock = [f"{s // 3600:02d}:{s // 60 % 60:02d}:{s % 60:02d}" for s in range(86400)]
    day = args.start_date.toordinal()
    date_prefix = args.start_date.isoformat() + "T"
    second = 0
    by_region = args.group_by == "region"
    bad_rate = args.bad_rate

    out.write((HEADER + "\n").encode())
    batch = []
    for tid in range(1, rows + 1):
        if len(batch) == BATCH_ROWS:
            out.write(("\n".join(batch) + "\n").encode())
            batch.clear()
        second += int(rnd() * 30) + 1
        if second >= 86400:
            second -= 86400
            day += 1
            date_prefix = datetime.date.fromordinal(day).isoformat() + "T"
        region = regions[int(rnd() * n_regions)]
        category = categories[int(rnd() * n_categories)]
        low, high = amounts[int(rnd() * n_amounts)]
        cents = low + int(rnd() * (high - low + 1))
        quantity = quantities[int(rnd() * n_quantities)]
        timestamp = date_prefix + clock[second]
        if bad_rate and rnd() < bad_rate:
            fields = [str(tid), timestamp, region, category, fmt_cents(cents), str(quantity)]
            spoil(fields, rng)
            batch.append(",".join(fields))
            if row_problem(fields) is not None:
                continue  # invalid rows are not part of the expected result
            # no kind of spoil() leaves a valid row, but such a row would count as written
            region, category, cents, quantity = (
                fields[2], fields[3], to_cents(fields[4]), int(fields[5]))
        else:
            batch.append(f"{tid},{timestamp},{region},{category},{fmt_cents(cents)},{quantity}")
        if groups is None:
            continue
        name = region if by_region else category
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
    if batch:
        out.write(("\n".join(batch) + "\n").encode())


def write_expected(f, groups):
    """The final result of a correct pipeline: groups sorted in byte order."""
    header = EXPECTED_HEADER
    lines = [header]
    for name in sorted(groups, key=lambda s: s.encode()):
        count, total, low, high, quantity = groups[name]
        # double arithmetic like awk's sum/count; may still differ by 0.01 at .xx5
        average = "%.2f" % ((total / 100) / count)
        line = (f"{name},{count},{fmt_cents(total)},{average},"
                f"{fmt_cents(low)},{fmt_cents(high)},{quantity}")
        lines.append(line)
    f.write("".join(line + "\n" for line in lines).encode())


def main(argv):
    args = parse_args(argv)
    rows = args.rows if args.rows is not None else args.size // BYTES_PER_ROW
    if args.seed is None:
        args.seed = random.SystemRandom().randrange(10 ** 9)
        print(f"gen_data.py: no --seed given, using --seed {args.seed}", file=sys.stderr)
    try:
        expected = open(args.expected, "wb") if args.expected else None
        out = open(args.output, "wb") if args.output else sys.stdout.buffer
    except OSError as e:
        print(f"gen_data.py: error: cannot write {e.filename}: {e.strerror}", file=sys.stderr)
        return 1

    groups = {} if expected else None
    try:
        generate(out, rows, args, groups)
        out.flush()
    except BrokenPipeError:
        # the reader went away (e.g. "| head"): stop quietly
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        return 1
    finally:
        if out is not sys.stdout.buffer:
            out.close()
    if expected:
        with expected:
            write_expected(expected, groups)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
