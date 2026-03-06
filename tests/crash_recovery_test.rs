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
/// Strategy (deterministic — no random message drops):
/// 1. Node 0 proposes value A for instance 0. Deliver Prepare ONLY to Node 2
///    (not Node 1, so Node 1's next_instance_id stays at 0).
/// 2. Deliver Promise from Node 2 back to Node 0. Node 0 reaches quorum (self + Node 2).
///    Accept(ballot=3, A) is emitted and self-delivered.
/// 3. Deliver Accept to Node 2 only. Node 2 accepts. AckAccept back to Node 0.
///    Node 0 reaches quorum of accepts → emits Learn(A).
/// 4. Deliver Learn to Node 2 → re-broadcasts → deliver back to Node 0. Node 0 learns A.
/// 5. CRASH Node 2.
/// 6. Restart Node 2 WITHOUT persistence → state lost.
/// 7. Node 1 proposes value B. Since next_instance_id=0, it gets instance 0 (same instance!).
///    Deliver Prepare ONLY to Node 2 (fresh). Node 2 promises without
///    reporting A (lost). Node 1 gets quorum without discovering A → proposes B.
/// 8. Complete Node 1's protocol with Node 2. Node 1 learns B.
/// 9. Node 0 has A, Node 1 has B → AGREEMENT VIOLATION.
fn run_targeted_crash_simulation(seed: u64, use_persistence: bool) -> (Vec<String>, String) {
    let _ = seed; // deterministic test, seed not used
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

    // === Phase 1: Node 0 proposes A for instance 0 ===
    let cmd_a = Cmd::Set("key".into(), "A".into());
    log.push_str(&format!("Step 1: Node 0 proposes {:?}\n", cmd_a));
    let (propose_msgs, _rx_a) = nodes[0].propose(cmd_a.clone()).unwrap();

    // Deliver Prepare ONLY to Node 2 (keep Node 1 isolated so next_instance_id stays 0)
    let mut promises = Vec::new();
    for msg in &propose_msgs {
        promises.extend(deliver_to(&mut nodes, msg, 2));
    }
    log.push_str(&format!(
        "  Delivered Prepare to Node 2 only. {} Promise responses\n",
        promises.len()
    ));

    // Deliver Promise from Node 2 to Node 0 → triggers Accept
    let mut accept_phase_msgs = Vec::new();
    for msg in &promises {
        accept_phase_msgs.extend(deliver_to(&mut nodes, msg, 0));
    }
    log.push_str(&format!(
        "  Delivered Promise to Node 0. {} Accept-phase messages\n",
        accept_phase_msgs.len()
    ));

    // Deliver Accept to Node 2 only → AckAccept
    let mut ack_msgs = Vec::new();
    for msg in &accept_phase_msgs {
        ack_msgs.extend(deliver_to(&mut nodes, msg, 2));
    }

    // Deliver AckAccept from Node 2 to Node 0 → triggers Learn
    let mut learn_msgs = Vec::new();
    for msg in &ack_msgs {
        learn_msgs.extend(deliver_to(&mut nodes, msg, 0));
    }

    // Deliver Learn to Node 2 → re-broadcasts
    let mut rebroadcasts = Vec::new();
    for msg in &learn_msgs {
        rebroadcasts.extend(deliver_to(&mut nodes, msg, 2));
    }

    // Deliver re-broadcast back to Node 0 → Node 0 learns A
    for msg in &rebroadcasts {
        let _ = deliver_to(&mut nodes, msg, 0);
    }

    let node0_learned = nodes[0].get_commit_id(0);
    log.push_str(&format!(
        "  Node 0 learned instance 0: {:?}\n",
        node0_learned.as_ref().map(|(cmd, _)| cmd)
    ));

    // === CRASH and RESTART Node 2 ===
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

    if use_persistence {
        log.push_str("\n=== RESTART: Node 2 recovers from storage ===\n");
        let ps = PaxosStorage::new(Box::new(SharedStorage(storages[2].clone())));
        nodes[2] = MultiPaxosNode::with_storage(make_config(2), ps).unwrap();
    } else {
        log.push_str("\n=== RESTART: Node 2 starts FRESH (state lost!) ===\n");
        nodes[2] = MultiPaxosNode::new(make_config(2));
    }

    // === Phase 2: Node 1 proposes B for instance 0 ===
    // Node 1 never saw any messages, so next_instance_id=0 → gets same instance!
    let cmd_b = Cmd::Set("key".into(), "B".into());
    log.push_str(&format!(
        "\nStep 2: Node 1 proposes {:?} (instance 0)\n",
        cmd_b
    ));
    let (propose_msgs_b, _rx_b) = nodes[1].propose(cmd_b.clone()).unwrap();

    // Deliver Node 1's Prepare ONLY to Node 2 (isolate from Node 0)
    let mut promises_b = Vec::new();
    for msg in &propose_msgs_b {
        promises_b.extend(deliver_to(&mut nodes, msg, 2));
    }
    log.push_str(&format!(
        "  Delivered Prepare to Node 2 only. {} responses\n",
        promises_b.len()
    ));

    // Deliver Promise from Node 2 to Node 1 → triggers Accept
    let mut accept_phase_b = Vec::new();
    for msg in &promises_b {
        accept_phase_b.extend(deliver_to(&mut nodes, msg, 1));
    }

    // Deliver Accept to Node 2 → AckAccept
    let mut ack_msgs_b = Vec::new();
    for msg in &accept_phase_b {
        ack_msgs_b.extend(deliver_to(&mut nodes, msg, 2));
    }

    // Deliver AckAccept from Node 2 to Node 1 → Learn
    let mut learn_msgs_b = Vec::new();
    for msg in &ack_msgs_b {
        learn_msgs_b.extend(deliver_to(&mut nodes, msg, 1));
    }

    // Deliver Learn to Node 2 → re-broadcast → deliver to Node 1 → Node 1 learns B
    let mut rebroadcasts_b = Vec::new();
    for msg in &learn_msgs_b {
        rebroadcasts_b.extend(deliver_to(&mut nodes, msg, 2));
    }
    for msg in &rebroadcasts_b {
        let _ = deliver_to(&mut nodes, msg, 1);
    }

    let node1_learned = nodes[1].get_commit_id(0);
    log.push_str(&format!(
        "  Node 1 learned instance 0: {:?}\n\n",
        node1_learned.as_ref().map(|(cmd, _)| cmd)
    ));

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
    // The targeted simulation is deterministic and always finds a violation
    // without persistence (Node 2 loses state, enabling a conflicting decision).
    let (violations, log) = run_targeted_crash_simulation(0, false);

    println!("\n{}", "=".repeat(70));
    if !violations.is_empty() {
        println!("CRASH WITHOUT PERSISTENCE — Agreement violation found (targeted)");
        println!("{}\n", "=".repeat(70));
        println!("{}", log);
    } else {
        println!("No violation found in targeted simulation.");
        println!("{}", "=".repeat(70));
        println!("{}", log);
    }

    assert!(
        !violations.is_empty(),
        "Expected to find an agreement violation without persistence in the targeted simulation"
    );
}

/// This test verifies that WITH persistence, no agreement violations occur
/// even with node crashes and restarts.
#[test]
fn test_crash_with_persistence_preserves_safety() {
    let mut all_violations = Vec::new();

    // Targeted simulation (deterministic, run once)
    let (v1, _) = run_targeted_crash_simulation(0, true);
    all_violations.extend(v1);

    // Randomized simulations
    let num_seeds = 2000;
    for seed in 0..num_seeds {
        let (v2, _) = run_crash_simulation(seed, true);
        all_violations.extend(v2);
    }

    assert!(
        all_violations.is_empty(),
        "Found {} agreement violations WITH persistence across {} randomized seeds!",
        all_violations.len(),
        num_seeds
    );
    println!(
        "✓ targeted + {} randomized seeds tested with crash+recovery (persistence enabled): no agreement violations",
        num_seeds
    );
}
