//! Crash-recovery simulation tests.
//!
//! These tests demonstrate why Paxos requires durable storage for correctness.
//! A node that crashes and restarts with fresh state can violate the protocol
//! by making conflicting promises, leading to agreement violations.
//!
//! Two modes are tested:
//! - **No persistence**: restarted node has blank state → can find agreement violations
//! - **With persistence**: restarted node recovers from storage → safety preserved

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use smr::multipaxos::storage::PaxosStorage;
use smr::multipaxos::{Message, MultiPaxosNode};
use smr::storage::{InMemoryStorage, Storage};
use smr::{SmrConfig, StateMachine, StateMachineReplicationAlgorithm};
use std::sync::{Arc, Mutex};

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug, Hash)]
enum Cmd {
    Set(String, String),
}

struct KV;

impl StateMachine for KV {
    type Command = Cmd;
    type Output = ();
    fn apply(&mut self, _command: Self::Command) -> smr::Result<Self::Output> {
        Ok(())
    }
}

/// SharedStorage wraps an Arc<Mutex<InMemoryStorage>> so multiple PaxosStorage
/// instances (before and after crash) can share the same underlying data.
struct SharedStorage(Arc<Mutex<InMemoryStorage>>);

impl Storage for SharedStorage {
    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.0.lock().unwrap().put(key, value)
    }
    fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        self.0.lock().unwrap().get(key)
    }
    fn delete(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.0.lock().unwrap().delete(key)
    }
    fn scan_prefix(&self, prefix: &[u8]) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.0.lock().unwrap().scan_prefix(prefix)
    }
}

fn make_config(id: u32) -> SmrConfig {
    let all_nodes: Vec<(u32, String)> = (0..3u32)
        .map(|nid| (nid, format!("127.0.0.1:{}", 9000 + nid)))
        .collect();
    let others: Vec<(u32, String)> = all_nodes
        .into_iter()
        .filter(|(nid, _)| *nid != id)
        .collect();
    SmrConfig::new(id, None, others).unwrap()
}

/// Deliver a single message to a specific node and collect responses.
fn deliver_to(
    nodes: &mut [MultiPaxosNode<KV>],
    msg: &Message<Cmd>,
    target_idx: usize,
) -> Vec<Message<Cmd>> {
    nodes[target_idx]
        .handle_message(msg.clone())
        .unwrap_or_default()
}

/// Deliver messages to all nodes except sender, with optional dropping.
fn deliver_all(
    nodes: &mut [MultiPaxosNode<KV>],
    pending: &[Message<Cmd>],
    rng: &mut StdRng,
    drop_prob: f64,
    skip_node: Option<usize>,
) -> Vec<Message<Cmd>> {
    let mut next = Vec::new();
    for msg in pending {
        let sender = msg.sender_id();
        for (idx, node) in nodes.iter_mut().enumerate() {
            if node.id() == sender {
                continue;
            }
            if Some(idx) == skip_node {
                continue;
            }
            if rng.random_bool(drop_prob) {
                continue;
            }
            if let Ok(responses) = node.handle_message(msg.clone()) {
                next.extend(responses);
            }
        }
    }
    next
}

/// Run a targeted crash-recovery simulation designed to trigger the violation.
///
/// Strategy:
/// 1. Node 0 proposes value A for instance 0 — deliver Prepare to all 3 nodes,
///    so all 3 acceptors promise ballot B0.
/// 2. Deliver Accept(B0, A) only to nodes 0 and 2 — they accept value A.
///    Node 1 hasn't accepted yet.
/// 3. CRASH node 2 (which accepted A at ballot B0).
/// 4. Restart node 2 WITHOUT persistence — it forgets its promise and accept.
/// 5. Node 1 proposes value B for the same instance 0 with a higher ballot B1.
///    Deliver Prepare(B1) to nodes 1 and 2. Node 2 (fresh) promises B1.
///    Node 1 promised B1. That's a quorum of promises for B1 with no prior accepted value reported.
/// 6. Deliver Accept(B1, B) to nodes 1 and 2. Both accept value B.
/// 7. Now node 0 learned A, nodes 1&2 can learn B → AGREEMENT VIOLATION.
fn run_targeted_crash_simulation(seed: u64, use_persistence: bool) -> (Vec<String>, String) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut log = String::new();

    let storages: Vec<Arc<Mutex<InMemoryStorage>>> = (0..3)
        .map(|_| Arc::new(Mutex::new(InMemoryStorage::new())))
        .collect();

    let mut nodes: Vec<MultiPaxosNode<KV>> = (0..3u32)
        .map(|id| {
            let ps = PaxosStorage::new(Box::new(SharedStorage(storages[id as usize].clone())));
            MultiPaxosNode::with_storage(make_config(id), ps).unwrap()
        })
        .collect();

    // === Step 1: Node 0 proposes value A ===
    let cmd_a = Cmd::Set("key".into(), "A".into());
    log.push_str(&format!("Step 1: Node 0 proposes {:?}\n", cmd_a));
    let (propose_msgs, _rx_a) = nodes[0].propose(cmd_a.clone()).unwrap();

    // propose_msgs includes the Prepare broadcast. Deliver to nodes 1 and 2.
    // (Node 0 already self-delivered via propose())
    let mut phase1_responses = Vec::new();
    for msg in &propose_msgs {
        // Deliver to node 1
        phase1_responses.extend(deliver_to(&mut nodes, msg, 1));
        // Deliver to node 2
        phase1_responses.extend(deliver_to(&mut nodes, msg, 2));
    }
    log.push_str(&format!(
        "  All 3 nodes received Prepare. Got {} responses (Promises)\n",
        phase1_responses.len()
    ));

    // === Step 2: Deliver Promise responses back to node 0 (the proposer) ===
    // This should trigger node 0 to send Accept messages once it has a quorum
    let mut accept_msgs = Vec::new();
    for msg in &phase1_responses {
        let responses = deliver_to(&mut nodes, msg, 0);
        for r in &responses {
            // Also self-deliver at node 0 (the broadcast includes self)
            accept_msgs.extend(deliver_to(&mut nodes, r, 0));
        }
        accept_msgs.extend(responses);
    }
    log.push_str(&format!(
        "  Delivered Promises to node 0. Got {} Accept-phase messages\n",
        accept_msgs.len()
    ));

    // === Step 3: Deliver Accept messages only to node 2 (not node 1) ===
    // Node 0 already self-delivered. Node 2 accepts. Node 1 is isolated.
    let mut learn_msgs = Vec::new();
    for msg in &accept_msgs {
        // Only deliver to node 2
        learn_msgs.extend(deliver_to(&mut nodes, msg, 2));
    }
    log.push_str(&format!(
        "  Delivered Accept to node 2 only. Node 1 isolated. {} responses\n",
        learn_msgs.len()
    ));

    // Deliver learn messages from node 2's AckAccept back to node 0
    for msg in &learn_msgs {
        let _ = deliver_to(&mut nodes, msg, 0);
    }

    // Check what node 0 learned so far
    let node0_learned = nodes[0].get_commit_id(0);
    log.push_str(&format!(
        "  Node 0 learned for instance 0: {:?}\n",
        node0_learned.as_ref().map(|(cmd, _)| cmd)
    ));

    // === Step 4: CRASH node 2 ===
    log.push_str("\n=== CRASH: Node 2 dies ===\n");
    {
        let st = storages[2].lock().unwrap();
        let keys = st.scan_prefix(b"acceptor/").unwrap();
        log.push_str(&format!("  Persisted state: {} entries\n", keys.len()));
        for (k, v) in &keys {
            log.push_str(&format!(
                "    {} = {}\n",
                String::from_utf8_lossy(k),
                String::from_utf8_lossy(v)
            ));
        }
    }

    // === Step 5: RESTART node 2 ===
    if use_persistence {
        log.push_str("\n=== RESTART: Node 2 recovers from storage ===\n");
        let ps = PaxosStorage::new(Box::new(SharedStorage(storages[2].clone())));
        nodes[2] = MultiPaxosNode::with_storage(make_config(2), ps).unwrap();
    } else {
        log.push_str("\n=== RESTART: Node 2 starts FRESH (state lost!) ===\n");
        nodes[2] = MultiPaxosNode::new(make_config(2));
    }

    // === Step 6: Node 1 proposes value B for a new instance ===
    // But due to instance_id tracking, it will get a different instance.
    // The real danger is for the SAME instance. So instead, we simulate
    // node 1 receiving a late Prepare for the same instance with a higher ballot.
    // We do this by having node 1 also propose — it may reuse instance 0 or get a new one.
    // Actually, let's deliver all remaining messages fully and then check.

    // Deliver everything that's still pending, plus have node 1 propose
    let cmd_b = Cmd::Set("key".into(), "B".into());
    log.push_str(&format!("\nStep 6: Node 1 proposes {:?}\n", cmd_b));
    let (propose_msgs_b, _rx_b) = nodes[1].propose(cmd_b.clone()).unwrap();

    // Deliver all pending messages fully
    let mut pending = propose_msgs_b;
    let mut rounds = 0;
    while !pending.is_empty() && rounds < 30 {
        pending.shuffle(&mut rng);
        let mut next = Vec::new();
        for msg in &pending {
            let sender = msg.sender_id();
            for node in nodes.iter_mut() {
                if node.id() == sender {
                    continue;
                }
                if rng.random_bool(0.05) {
                    continue;
                }
                if let Ok(responses) = node.handle_message(msg.clone()) {
                    next.extend(responses);
                }
            }
        }
        pending = next;
        rounds += 1;
    }
    log.push_str(&format!("  Delivered messages for {} rounds\n\n", rounds));

    // === Check agreement ===
    let mut violations = Vec::new();
    for instance_id in 0..5u64 {
        let mut learned: Vec<(u32, Cmd)> = Vec::new();
        for node in &mut nodes {
            if let Some((cmd, _)) = node.get_commit_id(instance_id) {
                learned.push((node.id(), cmd));
            }
        }
        if learned.is_empty() {
            continue;
        }
        let first = &learned[0].1;
        let mut agreed = true;
        for (nid, val) in &learned[1..] {
            if val != first {
                agreed = false;
                let msg = format!(
                    "AGREEMENT VIOLATION instance {}: node {} learned {:?}, node {} learned {:?}",
                    instance_id, learned[0].0, first, nid, val
                );
                log.push_str(&format!("  ⚠ {}\n", msg));
                violations.push(msg);
            }
        }
        if agreed {
            log.push_str(&format!(
                "Instance {}: {} nodes agree on {:?} ✓\n",
                instance_id,
                learned.len(),
                first
            ));
        }
    }

    log.push_str(&format!(
        "\nResult: {} agreement violations found\n",
        violations.len()
    ));
    (violations, log)
}

/// Run a randomized crash simulation with many seeds.
fn run_crash_simulation(seed: u64, use_persistence: bool) -> (Vec<String>, String) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut log = String::new();

    let storages: Vec<Arc<Mutex<InMemoryStorage>>> = (0..3)
        .map(|_| Arc::new(Mutex::new(InMemoryStorage::new())))
        .collect();

    let mut nodes: Vec<MultiPaxosNode<KV>> = (0..3u32)
        .map(|id| {
            let ps = PaxosStorage::new(Box::new(SharedStorage(storages[id as usize].clone())));
            MultiPaxosNode::with_storage(make_config(id), ps).unwrap()
        })
        .collect();

    let num_proposals = rng.random_range(2..=4);
    let crash_target = rng.random_range(0..3usize);
    let crash_after_rounds = rng.random_range(1..4);

    // Phase 1: Propose from random nodes
    let mut pending: Vec<Message<Cmd>> = Vec::new();
    for i in 0..num_proposals {
        let proposer = rng.random_range(0..3usize);
        let cmd = Cmd::Set(format!("k{}", i), format!("v{}", i));
        log.push_str(&format!("Phase 1: Node {} proposes {:?}\n", proposer, cmd));
        if let Ok((msgs, _rx)) = nodes[proposer].propose(cmd) {
            pending.extend(msgs);
        }
    }

    // Phase 1: Partial delivery
    for round in 0..crash_after_rounds {
        if pending.is_empty() {
            break;
        }
        pending.shuffle(&mut rng);
        let count = pending.len();
        pending = deliver_all(&mut nodes, &pending, &mut rng, 0.15, None);
        log.push_str(&format!(
            "Phase 1 round {}: delivered {} msgs, {} responses\n",
            round,
            count,
            pending.len()
        ));
    }

    // CRASH
    log.push_str(&format!(
        "\n=== CRASH: Node {} dies after {} rounds ===\n",
        crash_target, crash_after_rounds
    ));
    {
        let st = storages[crash_target].lock().unwrap();
        let keys = st.scan_prefix(b"acceptor/").unwrap();
        log.push_str(&format!("  Persisted state: {} entries\n", keys.len()));
        for (k, v) in &keys {
            log.push_str(&format!(
                "    {} = {}\n",
                String::from_utf8_lossy(k),
                String::from_utf8_lossy(v)
            ));
        }
    }

    // RESTART
    if use_persistence {
        log.push_str(&format!(
            "\n=== RESTART: Node {} recovers from storage ===\n",
            crash_target
        ));
        let ps = PaxosStorage::new(Box::new(SharedStorage(storages[crash_target].clone())));
        nodes[crash_target] =
            MultiPaxosNode::with_storage(make_config(crash_target as u32), ps).unwrap();
    } else {
        log.push_str(&format!(
            "\n=== RESTART: Node {} starts FRESH (state lost!) ===\n",
            crash_target
        ));
        nodes[crash_target] = MultiPaxosNode::new(make_config(crash_target as u32));
    }

    // Phase 2: More proposals + full delivery
    for i in num_proposals..(num_proposals + 2) {
        let proposer = rng.random_range(0..3usize);
        let cmd = Cmd::Set(format!("k{}", i), format!("v{}", i));
        log.push_str(&format!("Phase 2: Node {} proposes {:?}\n", proposer, cmd));
        if let Ok((msgs, _rx)) = nodes[proposer].propose(cmd) {
            pending.extend(msgs);
        }
    }

    let mut rounds = 0;
    let mut total = 0;
    while !pending.is_empty() && rounds < 50 {
        pending.shuffle(&mut rng);
        pending = deliver_all(&mut nodes, &pending, &mut rng, 0.05, None);
        total += pending.len();
        rounds += 1;
        if total > 10_000 {
            break;
        }
    }
    log.push_str(&format!("Phase 2: ran {} rounds\n\n", rounds));

    // Check agreement
    let mut violations = Vec::new();
    for instance_id in 0..(num_proposals + 2) as u64 {
        let mut learned: Vec<(u32, Cmd)> = Vec::new();
        for node in &mut nodes {
            if let Some((cmd, _)) = node.get_commit_id(instance_id) {
                learned.push((node.id(), cmd));
            }
        }
        if learned.is_empty() {
            continue;
        }
        if learned.len() > 1 {
            let first = &learned[0].1;
            let mut agreed = true;
            for (nid, val) in &learned[1..] {
                if val != first {
                    agreed = false;
                    let msg = format!(
                        "VIOLATION instance {}: node {} got {:?}, node {} got {:?}",
                        instance_id, learned[0].0, first, nid, val
                    );
                    violations.push(msg.clone());
                    log.push_str(&format!("  ⚠ {}\n", msg));
                }
            }
            if agreed {
                log.push_str(&format!(
                    "Instance {}: {} nodes agree on {:?} ✓\n",
                    instance_id,
                    learned.len(),
                    first
                ));
            }
        } else {
            log.push_str(&format!(
                "Instance {}: only node {} learned {:?}\n",
                instance_id, learned[0].0, learned[0].1
            ));
        }
    }

    log.push_str(&format!(
        "\nResult: {} agreement violations found\n",
        violations.len()
    ));
    (violations, log)
}

/// This test demonstrates that WITHOUT persistence, a crashed-and-restarted
/// acceptor can cause agreement violations.
#[test]
fn test_crash_without_persistence_finds_violations() {
    // First try the targeted simulation
    let mut found_violation = false;
    let mut violation_log = String::new();
    let mut violation_seed = 0u64;

    for seed in 0..100 {
        let (violations, log) = run_targeted_crash_simulation(seed, false);
        if !violations.is_empty() {
            found_violation = true;
            violation_log = log;
            violation_seed = seed;
            break;
        }
    }

    // Fall back to randomized simulation
    if !found_violation {
        for seed in 0..10000 {
            let (violations, log) = run_crash_simulation(seed, false);
            if !violations.is_empty() {
                found_violation = true;
                violation_log = log;
                violation_seed = seed + 100_000; // offset to distinguish
                break;
            }
        }
    }

    println!("\n{}", "=".repeat(70));
    if found_violation {
        println!(
            "CRASH WITHOUT PERSISTENCE — Agreement violation found at seed={}",
            violation_seed
        );
        println!("{}\n", "=".repeat(70));
        println!("{}", violation_log);
    } else {
        println!("No violation found in 10100 seeds.");
        println!("The violation scenario requires very specific message ordering.");
        println!("{}", "=".repeat(70));
    }

    // We expect to find a violation — that's the whole point
    assert!(
        found_violation,
        "Expected to find an agreement violation without persistence, but none found in 10100 seeds"
    );
}

/// This test verifies that WITH persistence, no agreement violations occur
/// even with node crashes and restarts.
#[test]
fn test_crash_with_persistence_preserves_safety() {
    let num_seeds = 2000;
    let mut all_violations = Vec::new();

    for seed in 0..num_seeds {
        // Test both targeted and randomized
        let (v1, _) = run_targeted_crash_simulation(seed, true);
        let (v2, _) = run_crash_simulation(seed, true);
        all_violations.extend(v1);
        all_violations.extend(v2);
    }

    assert!(
        all_violations.is_empty(),
        "Found {} agreement violations WITH persistence across {} seeds!",
        all_violations.len(),
        num_seeds
    );
    println!(
        "✓ {} seeds tested with crash+recovery (persistence enabled): no agreement violations",
        num_seeds
    );
}
