# SMR — State Machine Replication

## Build, test, and lint

```bash
cargo build
cargo test                           # all tests (unit + integration)
cargo test <test_name>               # single test, e.g. cargo test test_proposer
cargo test --test trace_test         # run only the TLA+ trace generation test
cargo fmt --all -- --check           # check formatting
cargo clippy --all-targets -- -D warnings  # lint (CI treats warnings as errors)
```

The kvstore example (3-node cluster):
```bash
cargo run --example kvstore -- 0 9000,9001,9002
```

## Architecture

```
SmrRuntime (lib.rs)                    ← User-facing runtime: propose commands, get results
  └── MultiPaxosNode (multipaxos/mod.rs)   ← Per-node Paxos orchestrator, manages instances
        └── PaxosInstance (synod/mod.rs)    ← Single-decree (Synod) Paxos round
              ├── Proposer (synod/proposer.rs)
              ├── Acceptor (synod/acceptor.rs)
              └── Learner  (synod/learner.rs)
  └── TcpChannel (channel.rs)              ← Length-prefixed bincode over TCP
```

Users implement the `StateMachine` trait (`Command` + `Output` + `apply()`), plug it into `SmrRuntime`, and call `propose()`. The runtime handles replication via Multi-Paxos so the same commands are applied in the same order on every node.

Currently "Multi-Paxos" runs a fresh Prepare→Promise→Accept→AckAccept→Learn for every proposal (repeated Synod, no stable leader). True Multi-Paxos leader optimization is a planned future step.

## Key conventions

**Proposer state machine** — The proposer uses a typestate pattern with `InnerState<C>` enum: `Initial → Proposal → Accepting → Learning`. Each state wraps a `StateWrapper<S>` carrying the ballot, quorum_size, and state-specific data. This makes illegal state transitions unrepresentable.

**Ballot encoding** — Ballots are globally unique per proposer: `(old_ballot / total_nodes + 1) * total_nodes + proposer_id`. This guarantees monotonically increasing, collision-free ballots without coordination.

**Self-deliver** — When a node proposes or handles a message, it processes the outgoing message through its own local PaxosInstance via `self_deliver()` before broadcasting. This ensures the local acceptor always votes on its own proposals and cascading responses (e.g., Accept after reaching a quorum of Promises) are generated immediately.

**TLA+ trace conformance** — Protocol linearization points emit structured events via `tracing::trace!(target: "paxos_trace", ...)` in `src/multipaxos/trace.rs`. These are zero-cost unless a subscriber is attached. The `tla/` directory contains TLA+ specs and tooling to replay traces through TLC for safety validation. When adding new protocol actions, add corresponding trace calls at linearization points.

**TcpChannel connection topology** — Each node only initiates outgoing connections to nodes with higher IDs. Lower-ID nodes accept incoming connections. Messages are length-prefixed bincode. `send()` currently broadcasts to all peers (no unicast targeting).

## Git

- NEVER run `git commit`. The user will review and handle commits.
- NEVER override or set git user.name or user.email.
