//! Trace generation tests for TLA+ conformance checking.
//!
//! - `test_trace_generation`: deterministic happy-path trace
//! - `test_randomized_traces`: randomized simulator that generates diverse traces
//!   by shuffling message delivery order, dropping messages, and having multiple
//!   competing proposers. Runs 50 seeds; on violation, re-runs with tracing
//!   enabled and dumps the full event log for debugging.

use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use smr::multipaxos::{Message, MultiPaxosNode};
use smr::{SmrConfig, StateMachine, StateMachineReplicationAlgorithm};
use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::prelude::*;

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug, Hash)]
enum Cmd {
    Set(String, String),
}

struct KV {
    store: std::collections::HashMap<String, String>,
}

impl StateMachine for KV {
    type Command = Cmd;
    type Output = ();

    fn apply(&mut self, command: Self::Command) -> smr::Result<Self::Output> {
        match command {
            Cmd::Set(k, v) => {
                self.store.insert(k, v);
            }
        }
        Ok(())
    }
}

/// A writer that appends to a shared buffer.
#[derive(Clone)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for SharedWriter {
    type Writer = SharedWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[test]
fn test_trace_generation() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let writer = SharedWriter(buf.clone());

    // Set up JSON tracing subscriber filtering for paxos_trace target
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_target(true)
        .with_level(false)
        .with_writer(writer)
        .with_filter(
            tracing_subscriber::filter::Targets::new()
                .with_target("paxos_trace", tracing::Level::TRACE),
        );

    let subscriber = tracing_subscriber::registry().with(fmt_layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let other_nodes: [(u32, String); 2] =
        [(1, "127.0.0.1:9001".into()), (2, "127.0.0.1:9002".into())];

    // Create 3 nodes
    let mut nodes: Vec<MultiPaxosNode<KV>> = (0..3u32)
        .map(|id| {
            let others: Vec<(u32, String)> = other_nodes
                .iter()
                .filter(|(nid, _)| *nid != id)
                .cloned()
                .chain(if other_nodes.iter().any(|(nid, _)| *nid == id) {
                    vec![]
                } else {
                    vec![(0, "127.0.0.1:9000".into())]
                })
                .collect();
            let config = SmrConfig::new(id, None, others).unwrap();
            MultiPaxosNode::new(config)
        })
        .collect();

    // Propose from node 0
    let (msgs, _rx) = {
        let node = &mut nodes[0];
        node.propose(Cmd::Set("key1".into(), "value1".into()))
            .unwrap()
    };

    // Deliver messages in rounds until no more messages
    let mut pending = msgs;
    let mut rounds = 0;
    while !pending.is_empty() && rounds < 20 {
        let mut next_pending = Vec::new();
        for msg in pending {
            let sender = msg.sender_id();
            for node in nodes.iter_mut() {
                if node.id() == sender {
                    continue;
                }
                match node.handle_message(msg.clone()) {
                    Ok(responses) => next_pending.extend(responses),
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
        }
        pending = next_pending;
        rounds += 1;
    }

    // Check that all nodes learned the value
    for node in &mut nodes {
        let commit = node.get_commit_id(0);
        assert!(
            commit.is_some(),
            "Node {} did not learn value for instance 0",
            node.id()
        );
    }

    // Parse captured JSON lines and extract trace events
    let trace_events = extract_trace_events(&buf);

    assert!(!trace_events.is_empty(), "No trace events captured");
    println!("Collected {} trace events:", trace_events.len());
    for evt in &trace_events {
        println!("  {}", evt);
    }

    // Write NDJSON trace file
    let trace_path = std::path::Path::new("/tmp/smr_trace.ndjson");
    let mut file = std::fs::File::create(trace_path).expect("Could not create trace file");
    for evt in &trace_events {
        use std::io::Write;
        serde_json::to_writer(&mut file, evt).unwrap();
        writeln!(file).unwrap();
    }
    println!("\nTrace written to {}", trace_path.display());
}

/// Create a 3-node cluster with consistent configuration.
fn make_cluster() -> Vec<MultiPaxosNode<KV>> {
    let all_nodes: Vec<(u32, String)> = (0..3u32)
        .map(|id| (id, format!("127.0.0.1:{}", 9000 + id)))
        .collect();

    all_nodes
        .iter()
        .map(|(id, _)| {
            let others: Vec<(u32, String)> = all_nodes
                .iter()
                .filter(|(nid, _)| nid != id)
                .cloned()
                .collect();
            let config = SmrConfig::new(*id, None, others).unwrap();
            MultiPaxosNode::new(config)
        })
        .collect()
}

/// Extract paxos_trace events from a shared JSON tracing buffer.
fn extract_trace_events(buf: &Arc<Mutex<Vec<u8>>>) -> Vec<serde_json::Value> {
    let raw = buf.lock().unwrap();
    let raw_str = String::from_utf8_lossy(&raw);
    let mut events = Vec::new();
    for line in raw_str.lines() {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(line)
            && val.get("target").and_then(|t| t.as_str()) == Some("paxos_trace")
            && let Some(fields) = val.get("fields")
        {
            events.push(fields.clone());
        }
    }
    events
}

/// Run one simulation with a seeded RNG. Returns any agreement violations found.
///
/// When `collect_trace` is true, installs a tracing subscriber to capture the full
/// paxos_trace event log (useful for debugging specific seeds).
///
/// Parameters control the chaos:
/// - `seed`: deterministic RNG seed (reproducible on failure)
/// - `num_proposals`: how many values to propose
/// - `drop_probability`: chance of dropping each message delivery (0.0–1.0)
/// - `max_rounds`: safety limit on delivery rounds
/// - `collect_trace`: whether to capture and return the trace event log
fn run_simulation(
    seed: u64,
    num_proposals: usize,
    drop_probability: f64,
    max_rounds: usize,
    collect_trace: bool,
) -> (Vec<String>, Vec<serde_json::Value>) {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let _guard = if collect_trace {
        let writer = SharedWriter(buf.clone());
        let fmt_layer = tracing_subscriber::fmt::layer()
            .json()
            .with_target(true)
            .with_level(false)
            .with_writer(writer)
            .with_filter(
                tracing_subscriber::filter::Targets::new()
                    .with_target("paxos_trace", tracing::Level::TRACE),
            );
        let subscriber = tracing_subscriber::registry().with(fmt_layer);
        Some(tracing::subscriber::set_default(subscriber))
    } else {
        None
    };

    let mut rng = StdRng::seed_from_u64(seed);
    let mut nodes = make_cluster();
    let mut violations = Vec::new();

    // Generate proposals from random nodes
    let mut pending: Vec<Message<Cmd>> = Vec::new();
    for i in 0..num_proposals {
        let proposer = rng.random_range(0..nodes.len());
        let cmd = Cmd::Set(format!("k{}", i), format!("v{}", i));
        match nodes[proposer].propose(cmd) {
            Ok((msgs, _rx)) => pending.extend(msgs),
            Err(e) => eprintln!("seed={seed}: propose error: {e}"),
        }
    }

    // Deliver messages with randomized ordering and drops
    let mut rounds = 0;
    let max_messages = 10_000;
    let mut total_messages = 0;
    while !pending.is_empty() && rounds < max_rounds {
        // Shuffle delivery order — this is where most interesting behavior comes from
        pending.shuffle(&mut rng);

        let mut next_pending = Vec::new();
        for msg in pending {
            let sender = msg.sender_id();
            for node in nodes.iter_mut() {
                if node.id() == sender {
                    continue;
                }
                // Randomly drop messages
                if rng.random_bool(drop_probability) {
                    continue;
                }
                match node.handle_message(msg.clone()) {
                    Ok(responses) => next_pending.extend(responses),
                    Err(e) => eprintln!("seed={seed}: handle error: {e}"),
                }
            }
        }
        pending = next_pending;
        total_messages += pending.len();
        rounds += 1;
        if total_messages > max_messages {
            break;
        }
    }

    // Verify agreement: for each instance, all nodes that learned must agree
    for instance_id in 0..num_proposals as u64 {
        let mut learned_values: Vec<(u32, Cmd)> = Vec::new();
        for node in &mut nodes {
            if let Some((cmd, _sender)) = node.get_commit_id(instance_id) {
                learned_values.push((node.id(), cmd));
            }
        }
        if learned_values.len() > 1 {
            let first = &learned_values[0].1;
            for (node_id, val) in &learned_values[1..] {
                if first != val {
                    violations.push(format!(
                        "seed={seed} instance={instance_id}: node {} learned {:?}, \
                         node {} learned {:?}",
                        learned_values[0].0, first, node_id, val
                    ));
                }
            }
        }
    }

    let trace_events = if collect_trace {
        extract_trace_events(&buf)
    } else {
        Vec::new()
    };

    (violations, trace_events)
}

#[test]
fn test_randomized_traces() {
    let num_seeds = 500;
    let mut all_violations = Vec::new();

    for seed in 0..num_seeds {
        let mut rng = StdRng::seed_from_u64(seed);
        let num_proposals = rng.random_range(1..=5);
        let drop_prob = rng.random_range(0.0..0.3);

        let (violations, _) = run_simulation(seed, num_proposals, drop_prob, 50, false);
        if !violations.is_empty() {
            // Re-run with tracing enabled to capture the full event log
            let (_, trace_events) = run_simulation(seed, num_proposals, drop_prob, 50, true);
            println!(
                "\n=== Trace for seed={seed} ({} events) ===",
                trace_events.len()
            );
            for evt in &trace_events {
                println!("  {}", evt);
            }
            all_violations.extend(violations);
        }
    }

    println!("\n=== Randomized Simulation Results ===");
    println!("Seeds tested: {num_seeds}");
    println!("Agreement violations found: {}", all_violations.len());
    if !all_violations.is_empty() {
        println!("\nViolations (reproduce with the given seed):");
        for v in &all_violations {
            println!("  ⚠ {v}");
        }
    }

    assert!(
        all_violations.is_empty(),
        "Found {} agreement violations",
        all_violations.len()
    );
}
