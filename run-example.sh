#!/usr/bin/env bash
# Usage: ./run-example.sh <example_name>
# Starts a 3-node cluster, Ctrl+C kills all instances.

set -e

EXAMPLE="${1:?Usage: $0 <example_name> (e.g. counter, kvstore)}"
PAXOS_PORTS="5000,5001,5002"

echo "Building $EXAMPLE..."
cargo build --example "$EXAMPLE"

PIDS=()

cleanup() {
    echo ""
    echo "Shutting down..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null
    echo "All nodes stopped."
}
trap cleanup EXIT INT TERM

for NODE_ID in 0 1 2; do
    HTTP_PORT=$((8080 + NODE_ID))
    echo "Starting node $NODE_ID (paxos=${PAXOS_PORTS}, http=:${HTTP_PORT})"
    ROCKET_PORT=$HTTP_PORT \
        cargo run --example "$EXAMPLE" -- "$NODE_ID" "$PAXOS_PORTS" 2>&1 | \
        sed "s/^/[node $NODE_ID] /" &
    PIDS+=($!)
done

echo ""
echo "=== Cluster running ==="
echo "  Node 0: http://localhost:8080"
echo "  Node 1: http://localhost:8081"
echo "  Node 2: http://localhost:8082"
echo ""
echo "Press Ctrl+C to stop all nodes."
echo ""

wait
