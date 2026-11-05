#!/usr/bin/env python3
"""Compare crud vs raw benchmark metrics.

Reads one or more benchmark JSON files given as positional arguments (use '-' to
read the JSON from STDIN, at most once). Each file is a separate benchmark run;
for every bench function the script aggregates the crud and raw throughput
values across all inputs and computes the ratio of the crud throughput to the
raw throughput, expressed as a percentage.

JSONL input is also supported: a '.jsonl' file (or any input whose content is
not a single JSON document) is parsed as JSON Lines, where every non-empty line
is treated as a separate benchmark run. This lets a single file contribute
several runs to the aggregation.

When more than one input is provided, the script verifies that every input
exposes the same selection of metrics (the same set of record keys) and warns
about any differences. Aggregated values are reported as medians: the text table
and the JSON document show median crud/raw throughputs and median ratios, while
the bencher measurement format ('--bmf') reports, for every metric, the median
as 'value' alongside 'lower_value' (min), 'upper_value' (max), 'values' (every
sample) and 'stdev' (sample standard deviation).

Pass '--json' to emit the report as a JSON document, or '--bmf' to emit the
aggregated metrics in bencher measurement format.

Each JSON record is keyed like:

    "%FUNCNAME%_bench::bench_%FUNCNAME%:crud"
    "%FUNCNAME%_bench::bench_%FUNCNAME%:raw"

The script verifies that every ':crud' record has a matching ':raw' record (and
vice versa), warns about any orphans, then prints the throughput ratios. The
':crud' / ':raw' suffix is the part after the last ':' of the key, so the pair
identifier (the "FUNCNAME") is everything before that suffix. Pairing by the
full prefix keeps the match correct even when the bench prefix and the bench
function name differ (e.g. "select_bench::bench_select_primary").

Usage:
    compare_metrics.py [--json|--bmf] <bench.json>... [->]
"""

import json
import re
import statistics
import sys

CRUD = "crud"
RAW = "raw"
SUFFIXES = (CRUD, RAW)

# A bench record key (without its :crud/:raw suffix) is expected to look like
# "<name>_bench::bench_<name>". We only warn about mismatches, never fail.
BENCH_KEY_RE = re.compile(r"^[^:]+_bench::bench_[^:]+$")


def parse_jsonl(label, text, warnings):
    """Parse JSON Lines text into a list of (run_label, data) pairs.

    Every non-empty line is decoded as an independent JSON document (one
    benchmark run). Blank lines are ignored. Lines that fail to decode produce
    a warning and are skipped. Run labels are '<label>:<lineno>' so warnings
    elsewhere can point back to the offending line.
    """
    runs = []
    for lineno, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError as exc:
            warnings.append(
                "invalid JSON on line {0} of '{1}': {2}".format(lineno, label, exc)
            )
            continue
        runs.append(("{0}:{1}".format(label, lineno), data))
    return runs


def parse_source_text(label, text, source, warnings):
    """Parse one input's text into a list of (run_label, data) pairs.

    A source whose name ends with '.jsonl' is always parsed as JSON Lines.
    Any other source is first tried as a single JSON document (one run); if
    that fails it falls back to JSON Lines, so multi-run streams without the
    '.jsonl' extension still work. If the fallback yields no runs the original
    JSON error is re-raised so genuinely broken inputs fail loudly.
    """
    if source.endswith(".jsonl"):
        return parse_jsonl(label, text, warnings)
    try:
        return [(label, json.loads(text))]
    except json.JSONDecodeError as exc:
        runs = parse_jsonl(label, text, warnings)
        if runs:
            return runs
        raise exc


def load_sources(args, warnings):
    """Load JSON/JSONL inputs from a list of file paths / '-' (STDIN).

    Returns a list of (run_label, data) pairs. STDIN ('-') is read at most once
    and reused if it appears more than once. Each '.jsonl' input (or any input
    that is not a single JSON document) expands into one pair per non-empty
    line. Raises FileNotFoundError or OSError as appropriate; malformed lines
    are reported via the warnings list instead of failing.
    """
    sources = []
    stdin_text = None
    stdin_seen = False
    for source in args:
        if source == "-":
            if not stdin_seen:
                stdin_text = sys.stdin.read()
                stdin_seen = True
            sources.extend(parse_source_text("stdin", stdin_text, source, warnings))
            continue
        with open(source, "r", encoding="utf-8") as handle:
            text = handle.read()
        sources.extend(parse_source_text(source, text, source, warnings))
    return sources


def split_key(key):
    """Split a benchmark key into (group, suffix).

    The suffix ('crud' or 'raw') follows the last ':' of the key. The group is
    everything before it and identifies the crud/raw pair.
    """
    group, _, suffix = key.rpartition(":")
    # rpartition returns ('', '', key) when there is no ':', which we treat as
    # an unknown suffix.
    if not group and not suffix:
        return key, None
    return group, suffix


def extract_function_name(group):
    """Return the bench function name for a pair group, or None.

    A group looks like "<prefix>_bench::bench_<name>"; the function name is the
    "<name>" part that follows "bench_" in the segment after "::". Returns None
    when the group does not match the expected shape.
    """
    _, _, func_part = group.partition("::")
    prefix = "bench_"
    if not func_part.startswith(prefix):
        return None
    return func_part[len(prefix):]


def get_throughput(record):
    """Return the 'throughput.value' of a record, or None if unavailable."""
    throughput = record.get("throughput") if isinstance(record, dict) else None
    if not isinstance(throughput, dict):
        return None
    value = throughput.get("value")
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    return value


def fmt_num(value):
    """Format a number with thousands separators, dropping a trailing .0."""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return "{:,}".format(value)


def compute_stats(values):
    """Aggregate a list of numeric samples into a stats dict.

    Returns None for an empty list. Otherwise reports the median as 'value',
    the minimum as 'lower_value', the maximum as 'upper_value', every sample
    as 'values' and the sample standard deviation as 'stdev' (0.0 when fewer
    than two samples are available).
    """
    if not values:
        return None
    return {
        "value": statistics.median(values),
        "lower_value": min(values),
        "upper_value": max(values),
        "values": list(values),
        "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
    }


def collect_groups(data, warnings):
    """Group records by their pair key, recording warnings for oddities."""
    groups = {}
    for key in sorted(data):
        record = data[key]
        group, suffix = split_key(key)

        if suffix not in SUFFIXES:
            warnings.append(
                "unrecognized record '{0}' "
                "(expected a ':crud' or ':raw' suffix, got ':{1}')".format(
                    key, suffix
                )
            )
            continue

        if not BENCH_KEY_RE.match(group):
            warnings.append(
                "record '{0}' does not look like a bench key "
                "(expected '<name>_bench::bench_<name>:<crud|raw>')".format(key)
            )

        if not isinstance(record, dict):
            warnings.append("record '{0}' is not an object, skipping".format(key))
            continue

        bucket = groups.setdefault(group, {})
        if suffix in bucket:
            warnings.append(
                "duplicate '{0}' record for '{1}' (keeping the first one)".format(
                    suffix, group
                )
            )
            continue
        bucket[suffix] = record

    return groups


def check_metric_consistency(named_keys, warnings):
    """Warn when inputs do not share the same set of record keys.

    named_keys is a list of (source, set_of_keys). Each input is compared
    against the union of keys across all inputs; any input missing keys that
    others provide produces a warning. Nothing happens for a single input.
    """
    if len(named_keys) <= 1:
        return
    union = set()
    for _, keys in named_keys:
        union |= keys
    for source, keys in named_keys:
        missing = sorted(union - keys)
        if missing:
            warnings.append(
                "'{0}' is missing {1} metric(s) present in other inputs: {2}".format(
                    source, len(missing), missing
                )
            )


def build_rows(file_groups, warnings):
    """Aggregate paired records across inputs into per-group sample lists.

    file_groups is a list of (source, groups). Returns rows of
    (group, crud_values, raw_values, ratios) where each *_values list collects
    one sample per input that provided the metric. ratios holds the per-input
    crud/raw percentage (only for inputs where both values are present and raw
    is non-zero).
    """
    all_groups = set()
    for _, groups in file_groups:
        all_groups.update(groups.keys())

    rows = []
    for group in sorted(all_groups):
        crud_values = []
        raw_values = []
        ratios = []
        for source, groups in file_groups:
            bucket = groups.get(group)
            if not bucket:
                continue
            crud_value = get_throughput(bucket.get(CRUD))
            raw_value = get_throughput(bucket.get(RAW))
            if crud_value is not None:
                crud_values.append(crud_value)
            if raw_value is not None:
                raw_values.append(raw_value)
            if crud_value is not None and raw_value is not None:
                if raw_value == 0:
                    warnings.append(
                        "'throughput.value' is 0 for '{0}:raw' in '{1}', "
                        "ratio undefined for that run".format(group, source)
                    )
                else:
                    ratios.append(crud_value / raw_value * 100.0)

        if not crud_values:
            warnings.append(
                "no 'throughput.value' for '{0}:crud' in any input, "
                "skipping".format(group)
            )
            continue
        if not raw_values:
            warnings.append(
                "no 'throughput.value' for '{0}:raw' in any input, "
                "skipping".format(group)
            )
            continue
        if not ratios:
            warnings.append(
                "no comparable crud/raw pair for '{0}' "
                "(ratio is undefined)".format(group)
            )

        rows.append((group, crud_values, raw_values, ratios))

    return rows


def build_report(rows, warnings):
    """Build a serializable report dict from computed rows and warnings."""
    median_ratios = [
        statistics.median(r[3]) for r in rows if r[3]
    ]
    report = {
        "results": [
            {
                "function": group,
                "crud_throughput": compute_stats(crud_values),
                "raw_throughput": compute_stats(raw_values),
                "ratio": compute_stats(ratios) if ratios else None,
            }
            for group, crud_values, raw_values, ratios in rows
        ],
        "summary": {
            "functions_compared": len(rows),
            "average_ratio": (sum(median_ratios) / len(median_ratios))
            if median_ratios
            else None,
            "lowest_ratio": None,
            "highest_ratio": None,
        },
        "warnings": list(warnings),
    }
    if median_ratios:
        best = min(rows, key=lambda r: statistics.median(r[3]) if r[3] else float("inf"))
        worst = max(rows, key=lambda r: statistics.median(r[3]) if r[3] else float("-inf"))
        report["summary"]["lowest_ratio"] = {
            "function": best[0],
            "ratio": statistics.median(best[3]),
        }
        report["summary"]["highest_ratio"] = {
            "function": worst[0],
            "ratio": statistics.median(worst[3]),
        }
    return report


def print_warnings(warnings):
    """Emit collected warnings to stderr so stdout stays parseable."""
    if not warnings:
        return
    sys.stderr.write("Warnings ({0}):\n".format(len(warnings)))
    for warning in warnings:
        sys.stderr.write("  - {0}\n".format(warning))
    sys.stderr.write("\n")


def print_table(rows):
    """Pretty-print the crud vs raw throughput comparison to stdout.

    Values shown are medians across all inputs. The 'Runs' column reports how
    many inputs contributed a comparable crud/raw pair for the ratio.
    """
    print("crud vs raw throughput comparison (median values)")
    print("=" * 52)
    print()

    if not rows:
        print("No paired crud/raw records found.")
        return

    func_col = max(len("Function"), max(len(r[0]) for r in rows))
    crud_col = max(
        len("crud (ops/s)"),
        max(len(fmt_num(statistics.median(r[1]))) for r in rows),
    )
    raw_col = max(
        len("raw (ops/s)"),
        max(len(fmt_num(statistics.median(r[2]))) for r in rows),
    )
    ratio_col = max(len("Ratio"), len("100.00%"), len("N/A"))
    runs_col = max(len("Runs"), max(len(str(len(r[3]))) for r in rows))

    def line(func, crud, raw, ratio, runs):
        return (
            f"{func:<{func_col}}  "
            f"{crud:>{crud_col}}  "
            f"{raw:>{raw_col}}  "
            f"{ratio:>{ratio_col}}  "
            f"{runs:>{runs_col}}"
        )

    separator = "  ".join(
        (
            "-" * func_col,
            "-" * crud_col,
            "-" * raw_col,
            "-" * ratio_col,
            "-" * runs_col,
        )
    )

    print(line("Function", "crud (ops/s)", "raw (ops/s)", "Ratio", "Runs"))
    print(separator)
    for group, crud_values, raw_values, ratios in rows:
        crud_median = statistics.median(crud_values)
        raw_median = statistics.median(raw_values)
        if ratios:
            ratio_str = "{0:.2f}%".format(statistics.median(ratios))
        else:
            ratio_str = "N/A"
        print(
            line(
                group,
                fmt_num(crud_median),
                fmt_num(raw_median),
                ratio_str,
                str(len(ratios)),
            )
        )
    print(separator)

    median_ratios = [statistics.median(r[3]) for r in rows if r[3]]
    print()
    print("Functions compared: {0}".format(len(rows)))
    if median_ratios:
        average = sum(median_ratios) / len(median_ratios)
        best = min(
            rows, key=lambda r: statistics.median(r[3]) if r[3] else float("inf")
        )
        worst = max(
            rows, key=lambda r: statistics.median(r[3]) if r[3] else float("-inf")
        )
        print("Average ratio:     {0:.2f}%".format(average))
        print(
            "Lowest ratio:      {1:.2f}%  ({0})".format(
                best[0], statistics.median(best[3])
            )
        )
        print(
            "Highest ratio:     {1:.2f}%  ({0})".format(
                worst[0], statistics.median(worst[3])
            )
        )


def print_json(report):
    """Print the report dict as a JSON document to stdout."""
    print(json.dumps(report, indent=2))


def build_bmf(rows, warnings):
    """Build a bencher measurement format dict from computed rows.

    The result maps each bench function name to its aggregated metrics. Every
    metric is a stats object with 'value' (median), 'lower_value' (min),
    'upper_value' (max), 'values' (every sample) and 'stdev' (sample standard
    deviation):

        {"<function>": {
            "throughput-ratio": {"value": <median>, "lower_value": <min>,
                                 "upper_value": <max>, "values": [...],
                                 "stdev": <stdev>},
            "throughput-crud":  {...},
            "throughput-raw":   {...},
        }, ...}

    Functions whose ratio could not be computed for any input (e.g. all raw
    throughputs are 0) keep their throughput-crud/throughput-raw stats but omit
    throughput-ratio; those cases are already reported via the warnings list.
    """
    bmf = {}
    for group, crud_values, raw_values, ratios in rows:
        function = extract_function_name(group)
        if function is None:
            warnings.append(
                "could not derive function name for '{0}', skipping in bmf".format(
                    group
                )
            )
            continue
        entry = {
            "throughput-crud": compute_stats(crud_values),
            "throughput-raw": compute_stats(raw_values),
        }
        if ratios:
            entry["throughput-ratio"] = compute_stats(ratios)
        # Order ratio first to match the previous output layout.
        ordered = {}
        if "throughput-ratio" in entry:
            ordered["throughput-ratio"] = entry["throughput-ratio"]
        ordered["throughput-crud"] = entry["throughput-crud"]
        ordered["throughput-raw"] = entry["throughput-raw"]
        bmf[function] = ordered
    return bmf


def parse_args(argv):
    """Parse argv into (mode, sources).

    Supported tokens: '--json' and '--bmf' (mutually exclusive output flags,
    may appear anywhere), '-h'/'--help', and one or more positional arguments
    (file paths, or '-' for STDIN at most once). Returns one of ("help", None),
    ("usage", None), ("table", sources), ("json", sources), or
    ("bmf", sources).
    """
    json_output = False
    bmf_output = False
    positional = []
    for arg in argv[1:]:
        if arg == "--json":
            json_output = True
        elif arg == "--bmf":
            bmf_output = True
        elif arg in ("-h", "--help"):
            return ("help", None)
        else:
            positional.append(arg)
    if json_output and bmf_output:
        return ("usage", None)
    if not positional:
        return ("usage", None)
    if bmf_output:
        return ("bmf", positional)
    if json_output:
        return ("json", positional)
    return ("table", positional)


def print_usage(prog, stream=sys.stderr):
    stream.write("usage: {0} [--json|--bmf] <bench.json>... [->]\n".format(prog))
    stream.write("  Parse benchmark JSON/JSONL file(s) and print aggregated ")
    stream.write("crud/raw throughput ratios.\n")
    stream.write("  Multiple files (and each line of a .jsonl file) are ")
    stream.write("aggregated as separate runs (median values).\n")
    stream.write("  Use '-' to read from STDIN (at most once).\n")
    stream.write("  Use '--json' to emit the result as a JSON document.\n")
    stream.write("  Use '--bmf' to emit the ratios in bencher measurement ")
    stream.write("format.\n")


def main(argv):
    prog = argv[0] if argv else __file__
    mode, sources = parse_args(argv)

    if mode == "help":
        print_usage(prog, stream=sys.stdout)
        return 0
    if mode == "usage":
        print_usage(prog)
        return 2

    warnings = []

    try:
        loaded = load_sources(sources, warnings)
    except FileNotFoundError as exc:
        sys.stderr.write("error: file not found: {0}\n".format(exc.filename))
        return 1
    except json.JSONDecodeError as exc:
        sys.stderr.write("error: invalid JSON: {0}\n".format(exc))
        return 1
    except OSError as exc:
        sys.stderr.write("error: could not read input: {0}\n".format(exc))
        return 1

    for source, data in loaded:
        if not isinstance(data, dict):
            sys.stderr.write(
                "error: top-level JSON value in '{0}' must be an object\n".format(
                    source
                )
            )
            return 1

    check_metric_consistency(
        [(source, set(data)) for source, data in loaded], warnings
    )

    file_groups = []
    for source, data in loaded:
        file_groups.append((source, collect_groups(data, warnings)))

    rows = build_rows(file_groups, warnings)

    if mode == "json":
        # Warnings are embedded in the JSON document; keep stderr quiet so the
        # JSON on stdout is the single source of truth for consumers.
        report = build_report(rows, warnings)
        print_json(report)
    elif mode == "bmf":
        # build_bmf may append more warnings; emit them to stderr so the JSON
        # on stdout stays a clean bencher measurement document.
        bmf = build_bmf(rows, warnings)
        print_warnings(warnings)
        print(json.dumps(bmf, indent=2))
    else:
        print_warnings(warnings)
        print_table(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
