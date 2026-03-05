# SMR — State Machine Replication

[![CI](https://github.com/FedericoPonzi/smr/actions/workflows/CI.yml/badge.svg)](https://github.com/FedericoPonzi/smr/actions/workflows/CI.yml)

A Rust library for studying and implementing state machine replication algorithms. Currently, supports (an incomplete
version of) Multi-Paxos.
Incomplete because it is not electing a leader yet, it is just running the synod protocol.

The library uses a replicated log: a cluster of nodes agrees on log entries through consensus. You define a state
machine and its commands — the library handles replication, so the same commands are applied in the same order on every
node.

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

let runtime = SmrRuntime::new(config, MyStateMachine::new()) ?;
let result = runtime.propose(my_command).await?;
```

## Examples!

The examples directory contains a number of working examples you can run easily by using the
`./run-example.sh <example>` script to start a cluster. They expose a REST API for interacting with the service.
Set `RUST_LOG=info` (or `RUST_LOG=smr=info`) to see the Paxos protocol in action.

### Distributed key-value store

The kvstore example uses [sled](https://docs.rs/sled) for persistent storage — data survives restarts. Storage is written to `data/kvstore-node-{id}/`.

```bash
# Start a 3-node cluster (in separate terminals, or use the helper script)
./run-example.sh kvstore

# Or manually:
cargo run --example kvstore --features sled -- 0 5000,5001,5002 &
cargo run --example kvstore --features sled -- 1 5000,5001,5002 &
cargo run --example kvstore --features sled -- 2 5000,5001,5002 &

# Write to any node, read from any node
curl -X POST -d "hello" http://localhost:8080/mykey
curl http://localhost:8081/mykey
```

### Distributed counter

```bash
# Start a 3-node cluster (in separate terminals, or use the helper script)
./run-example.sh counter
# Increment (returns new value)
curl -X POST http://localhost:8080/increment   # → 1
curl -X POST http://localhost:8081/increment   # → 2
# Decrement
curl -X POST http://localhost:8080/decrement   # → 1
# Read current value from any node
curl http://localhost:8082/value               # → 1
```

## How it works

Each node runs as a proposer, acceptor, and learner simultaneously. For now, there is no stable leader — any node can
propose a
value. The Paxos protocol ensures all nodes agree on the same sequence of commands:

1. **Prepare/Promise** — proposer picks a ballot, asks acceptors to promise
2. **Accept/AckAccept** — once a quorum promises, proposer asks them to accept a value
3. **Learn** — once a quorum accepts, the value is learned and applied

Nack messages allow fast recovery when ballots conflict.

## Testing

Testing is done at different levels: unit tests, integration tests, end-to-end tests, and simulation tests.

Simulation tests are used to generate random execution traces which are then tested against the correctness of the
consensus algorithm using TLA+. For more information on the last one, check the tla+
