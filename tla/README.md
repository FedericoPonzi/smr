# TLA+ Trace Validation

This directory contains TLA+ specifications and tooling for **trace-based conformance checking** of the SMR consensus
implementations.

## How it works

1. The Rust implementation emits structured trace events at protocol linearization points via `tracing::trace!()` with
   the `"paxos_trace"` target (zero cost when no subscriber is attached)
2. A TLA+ specification defines the correct protocol behavior (the "oracle")
3. A trace specification replays the collected trace step-by-step, constraining the TLC model checker to follow the
   implementation's execution
4. TLC verifies that each step is a valid protocol transition and that safety invariants (e.g., Agreement) hold

This technique is described
in ["Validating Traces of Distributed Programs Against TLA+ Specifications"](https://arxiv.org/abs/2404.16075) by Kuppe
et al. and is used in production by Microsoft CCF, etcd, and others.

## Prerequisites

- **Java 11+** (for TLC model checker)
- **tla2tools.jar** — download from [TLA+ releases](https://github.com/tlaplus/tlaplus/releases) and place in this
  directory
- **Python 3** (for the pipeline script)

## Quick start

```bash
# 1. Run the trace test to generate NDJSON
cargo test --test trace_test -- --nocapture

# 2. Generate TraceData.tla from the NDJSON (requires Node.js)
cat /tmp/smr_trace.ndjson | node -e '
  const lines = require("fs").readFileSync("/dev/stdin","utf8").trim().split("\n");
  const events = lines.map(l => JSON.parse(l));
  const recs = events.map(e =>
    `  [action |-> "${e.action}", node_id |-> ${e.node_id}, ballot |-> ${e.ballot}]`);
  console.log(`---- MODULE TraceData ----\nTraceData ==\n  <<\n${recs.join(",\n")}\n  >>\n====`);
' > tla/paxos/TraceData.tla

# 3. Run TLC to validate the trace against the Paxos spec
cd tla/paxos
java -Dtlc2.tool.queue.IStateQueue=StateDeque \
  -jar ../tla2tools.jar -config TracePaxos.cfg TracePaxos.tla
```

## Directory structure

```
tla/
  TraceUtils.tla          — Shared TLA+ helpers (reusable across algorithms)
  check_trace.py          — Generic trace-checking pipeline script
  README.md               — This file
  paxos/
    Paxos.tla             — Single-decree Paxos (Synod) specification
    TracePaxos.tla        — Trace validation spec for Paxos
    TracePaxos.cfg        — TLC configuration template
  raft/                   — (future) Raft specification + trace spec
  vsr/                    — (future) Viewstamped Replication spec + trace spec
```

## Adding a new algorithm

1. Create `tla/<algorithm>/` directory
2. Write the protocol spec: `tla/<algorithm>/<Algorithm>.tla`
3. Write the trace spec: `tla/<algorithm>/Trace<Algorithm>.tla`
4. Add trace helper functions in Rust: `src/<algorithm>/trace.rs` using `tracing::trace!(target: "<alg>_trace", ...)`
5. Instrument the Rust code at linearization points
6. Run: `python tla/check_trace.py --algorithm <algorithm> --trace <file.ndjson>`

## How to read TLC output

- **✅ No errors**: The trace is a valid behavior of the specification
- **❌ Invariant violated**: TLC found a step where a safety property was broken. The output shows the exact state and
  step number
- **❌ Deadlock**: The trace contains a step that doesn't match any valid spec transition. This usually means the
  implementation did something the spec doesn't allow

## References

- [TLA+ Trace Validation Wiki](https://docs.tlapl.us/using:tlc:trace_validation)
- [Kuppe et al. — Validating Traces (arXiv:2404.16075)](https://arxiv.org/abs/2404.16075)
- [Microsoft CCF TLA+ specs](https://github.com/microsoft/CCF/tree/main/tla/consensus)
- [EWD998 trace validation example](https://github.com/tlaplus/Examples/tree/master/specifications/ewd998)
- [raft-rs trace discussion](https://github.com/eatonphil/raft-rs/issues/1#issuecomment-1854576123)
