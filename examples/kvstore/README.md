# Example Key-Value Store

Example replicated kv store that exposes a rest api.

All reads and writes go through smr.

## Running

The cluster requires at least 3 nodes but supports any number >= 3.
Each node gets a unique ID (starting from 0) and the same comma-separated list of SMR ports.
The HTTP API port defaults to `8080 + node_id` (override with `ROCKET_PORT`).

### 3-node cluster

```bash
# Terminal 1
cargo run --example kvstore 0 9000,9001,9002

# Terminal 2
cargo run --example kvstore 1 9000,9001,9002

# Terminal 3
cargo run --example kvstore 2 9000,9001,9002
```

### 4-node cluster (fault-tolerant to 1 failure)

```bash
# Terminal 1
cargo run --example kvstore 0 9000,9001,9002,9003

# Terminal 2
cargo run --example kvstore 1 9000,9001,9002,9003

# Terminal 3
cargo run --example kvstore 2 9000,9001,9002,9003

# Terminal 4
cargo run --example kvstore 3 9000,9001,9002,9003
```

You can kill any one node and the remaining 3 will continue operating (a majority quorum of 3 out of 4 is still reachable).

## Client API

Set a value:

```
curl -X POST -d "value1" http://localhost:8080/key1  # Set key1 to value1 on Node 0
curl -X POST -d "value2" http://localhost:8081/key2  # Set key2 to value2 on Node 1
curl -X POST -d "value3" http://localhost:8082/key3  # Set key3 to value3 on Node 2

curl http://localhost:8080/key1  # Get key1 from Node 0 (should return value1)
curl http://localhost:8081/key2  # Get key2 from Node 1 (should return value2)
curl http://localhost:8082/key3  # Get key3 from Node 2 (should return value3)
```

Consistency test:

```
# Set a value on Node 1
curl -X POST -d "consistent_value" http://localhost:8081/consistent_key

# Get the value from all nodes. They should eventually return "consistent_value"
curl http://localhost:8080/consistent_key
curl http://localhost:8081/consistent_key
curl http://localhost:8082/consistent_key
```