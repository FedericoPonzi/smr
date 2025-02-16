# Example Key-Value Store

Example replicated kv store that exposes a rest api.

All reads and writes go through smr.

## Running

```bash
# Terminal 1 (Node 0, ports 8080, 8081, 8082)
cargo run --example kvstore 0 8080,8081,8082

# Terminal 2 (Node 1, ports 8080, 8081, 8082)
cargo run --example kvstore 1 8080,8081,8082

# Terminal 3 (Node 2, ports 8080, 8081, 8082)
cargo run --example kvstore 2 8080,8081,8082
```

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
curl http://localhost:8080/consistent_key  # Might be empty initially, but will eventually return "consistent_value"
curl http://localhost:8081/consistent_key  # Might be empty initially, but will eventually return "consistent_value"
curl http://localhost:8082/consistent_key  # Might be empty initially, but will eventually return "consistent_value"
```