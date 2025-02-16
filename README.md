# State machine replication

A library that offers different algorithms for state machine replication.

It aims to offer a correct and efficient implementations to solve consensus. Currently, it only supports Paxos.

This library works by using a replicated log. A cluster of nodes agrees on what the next log entry should be.

As a user you can use this library to build any replicated state machine. Each Log entry is a command.
A command will move the machine from the current state to the next.

## Examples:

```
cargo build --example kvstore
```

## Testing

Testing is done through unit testing, integration testing and using [Shuttle](https://github.com/awslabs/shuttle)
library.

## References:

---