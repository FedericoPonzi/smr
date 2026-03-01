# SMR — State Machine Replication

[![CI](https://github.com/FedericoPonzi/smr/actions/workflows/CI.yml/badge.svg)](https://github.com/FedericoPonzi/smr/actions/workflows/CI.yml)

A Rust library for studying and implementing state machine replication algorithms. Currently supports Multi-Paxos.

The library uses a replicated log: a cluster of nodes agrees on log entries through consensus. You define a state machine and its commands — the library handles replication, so the same commands are applied in the same order on every node.

## Quick start

The library exposes a `StateMachine` trait. Implement it, plug it into `SmrRuntime`, and you have a replicated service:

```rust
impl StateMachine for MyStateMachine {
    type Command = MyCommand;
    type Output = MyOutput;

    fn apply(&mut self, command: Self::Command) -> Result<Self::Output> {
        // your logic here
    }
}

let runtime = SmrRuntime::new(config, MyStateMachine::new())?;
let result = runtime.propose(my_command).await?;
```

## Example: distributed key-value store

A working example is included in `examples/kvstore/`. It runs a 3-node replicated KV store over HTTP using Rocket.

```bash
# Terminal 1-3: start the cluster
cargo run --example kvstore -- 0 9000,9001,9002
cargo run --example kvstore -- 1 9000,9001,9002
cargo run --example kvstore -- 2 9000,9001,9002

# Write to any node, read from any node
curl -X POST -d "hello" http://localhost:8080/mykey
curl http://localhost:8081/mykey
```

Set `RUST_LOG=info` (or `RUST_LOG=smr=info`) to see the Paxos protocol in action.

## How it works

Each node runs as a proposer, acceptor, and learner simultaneously. There is no stable leader — any node can propose a value. The Multi-Paxos protocol ensures all nodes agree on the same sequence of commands:

1. **Prepare/Promise** — proposer picks a ballot, asks acceptors to promise
2. **Accept/AckAccept** — once a quorum promises, proposer asks them to accept a value
3. **Learn** — once a quorum accepts, the value is learned and applied

Nack messages allow fast recovery when ballots conflict.

## Tests

```bash
cargo test
```
