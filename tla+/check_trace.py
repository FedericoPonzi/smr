#!/usr/bin/env python3
"""
Generic trace-checking pipeline for TLA+ conformance validation.

Reads NDJSON traces from the SMR implementation, converts them to a TLA+
Trace constant, generates a TLC config, and runs TLC to check conformance.

Usage:
    python tla/check_trace.py --algorithm paxos --trace traces/run1.ndjson [--trace traces/run2.ndjson ...]

Prerequisites:
    - Java 11+ (for TLC)
    - tla2tools.jar (download from https://github.com/tlaplus/tlaplus/releases)
      Place in tla/ directory or set TLA2TOOLS_JAR env var.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def load_ndjson_traces(trace_files):
    """Load and merge NDJSON trace files, sorted by timestamp."""
    events = []
    for path in trace_files:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    events.append(json.loads(line))
    events.sort(key=lambda e: e.get("timestamp_ns", 0))
    return events


def tla_value(v):
    """Convert a Python value to a TLA+ value string."""
    if isinstance(v, str):
        return f'"{v}"'
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    if v is None:
        return "-1"  # Use -1 for None/missing values in TLA+
    return f'"{v}"'


def events_to_tla_trace(events):
    """Convert trace events to a TLA+ sequence of records."""
    records = []
    for evt in events:
        fields = []
        for key in ["action", "node_id", "instance_id", "ballot", "sender",
                     "max_bal", "max_v_bal", "max_val"]:
            val = evt.get(key)
            if val is not None:
                fields.append(f"{key} |-> {tla_value(val)}")
            else:
                # Use sentinel values for missing fields
                if key in ("max_bal", "max_v_bal", "ballot", "sender"):
                    fields.append(f"{key} |-> -1")
                elif key in ("max_val",):
                    fields.append(f'{key} |-> "None"')
        records.append(f"[{', '.join(fields)}]")
    return f"<<\n  {(',{nl}  '.format(nl=chr(10))).join(records)}\n>>"


def extract_constants(events, algorithm):
    """Extract model constants from the trace events."""
    nodes = set()
    values = set()
    for evt in events:
        nodes.add(evt.get("node_id", 0))
        if evt.get("sender") is not None:
            nodes.add(evt["sender"])
        if evt.get("max_val") and evt["max_val"] != "None":
            values.add(evt["max_val"])

    if not values:
        values.add("default")

    quorum = len(nodes) // 2 + 1
    return {
        "Nodes": "{" + ", ".join(str(n) for n in sorted(nodes)) + "}",
        "Values": "{" + ", ".join(f'"{v}"' for v in sorted(values)) + "}",
        "Quorum": str(quorum),
    }


def generate_cfg(constants, trace_tla, cfg_path):
    """Generate TLC .cfg file."""
    lines = [
        f"SPECIFICATION TraceSpec",
        f"",
        f"INVARIANT TraceAgreement",
        f"",
    ]
    for name, val in constants.items():
        lines.append(f"CONSTANT {name} = {val}")
    lines.append(f"")
    lines.append(f"CONSTANT Trace = {trace_tla}")
    lines.append(f"")

    with open(cfg_path, "w") as f:
        f.write("\n".join(lines))


def find_tla2tools():
    """Find tla2tools.jar."""
    # Check env var
    jar = os.environ.get("TLA2TOOLS_JAR")
    if jar and os.path.exists(jar):
        return jar

    # Check common locations
    script_dir = Path(__file__).parent
    for candidate in [
        script_dir / "tla2tools.jar",
        Path.home() / "tla2tools.jar",
        Path("/usr/local/lib/tla2tools.jar"),
    ]:
        if candidate.exists():
            return str(candidate)

    return None


def run_tlc(algorithm, cfg_path, tla_dir):
    """Run TLC model checker."""
    jar = find_tla2tools()
    if not jar:
        print("ERROR: tla2tools.jar not found.", file=sys.stderr)
        print("  Download from: https://github.com/tlaplus/tlaplus/releases", file=sys.stderr)
        print("  Place in tla/ directory or set TLA2TOOLS_JAR env var.", file=sys.stderr)
        return False

    trace_spec = tla_dir / algorithm / f"Trace{algorithm.capitalize()}.tla"
    if not trace_spec.exists():
        # Try exact case
        for f in (tla_dir / algorithm).iterdir():
            if f.name.lower() == f"trace{algorithm.lower()}.tla":
                trace_spec = f
                break

    cmd = [
        "java",
        "-Dtlc2.tool.queue.IStateQueue=StateDeque",  # DFS for trace validation
        "-jar", jar,
        "-config", str(cfg_path),
        "-workers", "1",
        str(trace_spec),
    ]

    print(f"Running TLC: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

    print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)

    # Check for violations
    if "Error:" in result.stdout or "Invariant" in result.stdout and "violated" in result.stdout:
        print("\n❌ CONFORMANCE CHECK FAILED", file=sys.stderr)
        return False

    if result.returncode != 0:
        print(f"\n❌ TLC exited with code {result.returncode}", file=sys.stderr)
        return False

    print("\n✅ Trace conforms to specification")
    return True


def main():
    parser = argparse.ArgumentParser(description="TLA+ trace conformance checker")
    parser.add_argument("--algorithm", "-a", required=True,
                        help="Algorithm name (e.g., paxos, raft, vsr)")
    parser.add_argument("--trace", "-t", required=True, action="append",
                        help="NDJSON trace file(s) (can specify multiple)")
    parser.add_argument("--tla-dir", default=None,
                        help="Path to tla/ directory (default: auto-detect)")
    args = parser.parse_args()

    # Find tla/ directory
    if args.tla_dir:
        tla_dir = Path(args.tla_dir)
    else:
        tla_dir = Path(__file__).parent
    if not (tla_dir / args.algorithm).is_dir():
        print(f"ERROR: {tla_dir / args.algorithm} not found", file=sys.stderr)
        sys.exit(1)

    # Load traces
    print(f"Loading traces from: {args.trace}")
    events = load_ndjson_traces(args.trace)
    print(f"Loaded {len(events)} events from {len(args.trace)} file(s)")

    if not events:
        print("WARNING: No trace events found. Nothing to check.")
        sys.exit(0)

    # Extract constants and generate TLA+ trace
    constants = extract_constants(events, args.algorithm)
    trace_tla = events_to_tla_trace(events)

    print(f"Model constants: {constants}")

    # Write config to temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".cfg", delete=False,
                                      dir=str(tla_dir / args.algorithm)) as f:
        cfg_path = f.name
        generate_cfg(constants, trace_tla, cfg_path)

    try:
        success = run_tlc(args.algorithm, cfg_path, tla_dir)
    finally:
        os.unlink(cfg_path)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
