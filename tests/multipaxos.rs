use serde::{Deserialize, Serialize};
use smr::multipaxos::{MessageKind, MultiPaxosNode};
use smr::{SmrConfig, StateMachine, StateMachineReplicationAlgorithm};
use tracing::debug;

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug, Hash)]
enum Command {
    Set(u32),
    Get(u32),
}
struct MySM {
    values: Vec<u32>,
}
impl StateMachine for MySM {
    type Command = Command;
    type Output = ();

    fn apply(&mut self, command: Self::Command) -> smr::Result<Self::Output> {
        match command {
            Command::Set(value) => {
                self.values.push(value);
            }
            Command::Get(value) => {
                let _ign = self.values.contains(&value);
            }
        };
        Ok(())
    }
}

fn make_config(id: u32) -> SmrConfig {
    let peers: Vec<(u32, String)> = (0..3u32)
        .filter(|&i| i != id)
        .map(|i| (i, format!("127.0.0.1:{}", 9000 + i)))
        .collect();
    SmrConfig::new(id, None, peers).unwrap()
}

/// Deliver messages to all nodes except the sender, return all responses.
fn deliver_to_others(
    nodes: &mut [MultiPaxosNode<MySM>],
    msgs: &[smr::multipaxos::Message<Command>],
) -> Vec<smr::multipaxos::Message<Command>> {
    let mut responses = Vec::new();
    for msg in msgs {
        let sender = msg.sender_id();
        for node in nodes.iter_mut() {
            if node.id() != sender
                && let Ok(r) = node.handle_message(msg.clone())
            {
                responses.extend(r);
            }
        }
    }
    responses
}

/// Deliver messages only to a specific node, return responses.
fn deliver_to(
    node: &mut MultiPaxosNode<MySM>,
    msgs: &[smr::multipaxos::Message<Command>],
) -> Vec<smr::multipaxos::Message<Command>> {
    let mut responses = Vec::new();
    for msg in msgs {
        if let Ok(r) = node.handle_message(msg.clone()) {
            responses.extend(r);
        }
    }
    responses
}

#[test]
fn test_smr_multipaxos() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    let other_nodes = vec![(2, "123".into()), (3, "124".into()), (4, "125".into())];
    let mut mpn1: MultiPaxosNode<MySM> = smr::multipaxos::MultiPaxosNode::new(
        smr::SmrConfig::new(1, None, other_nodes.clone()).unwrap(),
    );
    let _mpn2: MultiPaxosNode<MySM> =
        smr::multipaxos::MultiPaxosNode::new(smr::SmrConfig::new(2, None, other_nodes).unwrap());
    let _out = mpn1.propose(Command::Set(42)).unwrap();
    let _out = mpn1.propose(Command::Set(42)).unwrap();
    let _out = mpn1.propose(Command::Set(42)).unwrap();
    let out = mpn1.propose(Command::Set(42)).unwrap();
    debug!("{:?}", out.0);
}

/// Node proposes, full Paxos round completes → becomes leader.
#[test]
fn test_leader_after_successful_proposal() {
    let mut nodes: Vec<MultiPaxosNode<MySM>> = (0..3u32)
        .map(|id| MultiPaxosNode::new(make_config(id)))
        .collect();
    assert!(nodes[0].leader_ballot().is_none());

    // Node 0 proposes → Prepare + self-delivered Promise
    let (propose_msgs, _rx) = nodes[0].propose(Command::Set(42)).unwrap();

    // Deliver to nodes 1, 2 → they respond with Promises
    let phase1_responses = deliver_to_others(&mut nodes, &propose_msgs);

    // Deliver Promises back to Node 0 → proposer produces Accept
    let phase2_msgs = deliver_to(&mut nodes[0], &phase1_responses);

    // Not leader yet — Phase 2 hasn't completed
    assert!(nodes[0].leader_ballot().is_none());

    // Deliver Accept to nodes 1, 2 → they respond with AckAccept
    let phase2_responses = deliver_to_others(&mut nodes, &phase2_msgs);

    // Deliver AckAccepts to Node 0 → proposer produces Learn
    let _learn_msgs = deliver_to(&mut nodes[0], &phase2_responses);

    // Now Node 0 should be leader
    assert!(
        nodes[0].leader_ballot().is_some(),
        "Node should become leader after completing a full Paxos round"
    );
}

/// Node is leader, then another node proposes with a higher ballot → old leader
/// loses leadership when its acceptor promises the higher ballot.
#[test]
fn test_leader_lost_on_higher_ballot() {
    let mut nodes: Vec<MultiPaxosNode<MySM>> = (0..3u32)
        .map(|id| MultiPaxosNode::new(make_config(id)))
        .collect();

    // Complete a full round for Node 0 → becomes leader
    let (msgs, _rx) = nodes[0].propose(Command::Set(1)).unwrap();
    let r1 = deliver_to_others(&mut nodes, &msgs);
    let r2 = deliver_to(&mut nodes[0], &r1);
    let r3 = deliver_to_others(&mut nodes, &r2);
    let _r4 = deliver_to(&mut nodes[0], &r3);
    assert!(nodes[0].leader_ballot().is_some());

    // Node 1 proposes → sends Prepare with higher ballot
    let (msgs_1, _rx) = nodes[1].propose(Command::Set(2)).unwrap();

    // Deliver Node 1's Prepare to Node 0 → Node 0 promises higher ballot → loses leadership
    let _ = deliver_to(&mut nodes[0], &msgs_1);

    assert!(
        nodes[0].leader_ballot().is_none(),
        "Node 0 should lose leadership after promising a higher ballot"
    );
}

/// Proposal starts but doesn't complete — node should NOT be leader.
#[test]
fn test_no_leader_without_round_completion() {
    let mut nodes: Vec<MultiPaxosNode<MySM>> = (0..3u32)
        .map(|id| MultiPaxosNode::new(make_config(id)))
        .collect();

    // Node 0 proposes but we only deliver Phase 1 (no Phase 2 completion)
    let (msgs, _rx) = nodes[0].propose(Command::Set(42)).unwrap();
    let r1 = deliver_to_others(&mut nodes, &msgs);
    let _r2 = deliver_to(&mut nodes[0], &r1); // Gets Accept, but no AckAccepts yet

    assert!(
        nodes[0].leader_ballot().is_none(),
        "Node should NOT be leader until a full round completes"
    );
}

/// Run a full Paxos round: propose on `proposer_idx`, deliver through all phases.
fn run_full_round(nodes: &mut [MultiPaxosNode<MySM>], proposer_idx: usize, cmd: Command) {
    let (msgs, _rx) = nodes[proposer_idx].propose(cmd).unwrap();
    let r1 = deliver_to_others(nodes, &msgs);
    let r2 = deliver_to(&mut nodes[proposer_idx], &r1);
    let r3 = deliver_to_others(nodes, &r2);
    let r4 = deliver_to(&mut nodes[proposer_idx], &r3);
    // Deliver Learn to all peers + re-broadcasts back for quorum
    let r5 = deliver_to_others(nodes, &r4);
    let _ = deliver_to_others(nodes, &r5);
}

/// Non-leader propose() should return a RequestCommandToLeader message.
#[test]
fn test_non_leader_propose_forwards() {
    let mut nodes: Vec<MultiPaxosNode<MySM>> = (0..3u32)
        .map(|id| MultiPaxosNode::new(make_config(id)))
        .collect();

    // Node 0 wins a round → becomes leader
    run_full_round(&mut nodes, 0, Command::Set(1));
    assert!(nodes[0].leader_ballot().is_some());
    assert!(nodes[1].leader_ballot().is_none());

    // Node 1 (non-leader) calls propose → should get a forward message, not Prepare
    let (msgs, _rx) = nodes[1].propose(Command::Set(99)).unwrap();
    assert!(!msgs.is_empty());
    let has_forward = msgs
        .iter()
        .any(|m| matches!(m.clone().kind(), MessageKind::RequestCommandToLeader(_)));
    assert!(
        has_forward,
        "Non-leader should produce RequestCommandToLeader, got: {:?}",
        msgs.iter().map(|m| format!("{:?}", m)).collect::<Vec<_>>()
    );
}

/// Leader handles a forwarded RequestCommandToLeader and runs Paxos for it.
#[test]
fn test_leader_handles_forwarded_proposal() {
    let mut nodes: Vec<MultiPaxosNode<MySM>> = (0..3u32)
        .map(|id| MultiPaxosNode::new(make_config(id)))
        .collect();

    // Node 0 wins a round → becomes leader
    run_full_round(&mut nodes, 0, Command::Set(1));
    assert!(nodes[0].leader_ballot().is_some());

    // Node 1 (non-leader) proposes → gets forward message
    let (forward_msgs, _rx) = nodes[1].propose(Command::Set(99)).unwrap();

    // Deliver forward message to leader (Node 0) → should produce Prepare
    let leader_msgs = deliver_to(&mut nodes[0], &forward_msgs);
    let has_prepare = leader_msgs
        .iter()
        .any(|m| matches!(m.clone().kind(), MessageKind::PrepareMsg(_)));
    assert!(
        has_prepare,
        "Leader should start Paxos for the forwarded command"
    );

    // Complete the round: deliver through all phases
    let r1 = deliver_to_others(&mut nodes, &leader_msgs);
    let r2 = deliver_to(&mut nodes[0], &r1);
    let r3 = deliver_to_others(&mut nodes, &r2);
    let r4 = deliver_to(&mut nodes[0], &r3);
    let r5 = deliver_to_others(&mut nodes, &r4);
    // Learn re-broadcasts need one more round for quorum
    let _ = deliver_to_others(&mut nodes, &r5);

    // Debug: find which instance the leader used
    let forwarded_instance = leader_msgs
        .iter()
        .find_map(|m| {
            if matches!(m.clone().kind(), MessageKind::PrepareMsg(_)) {
                Some(m.instance_id())
            } else {
                None
            }
        })
        .expect("should have a Prepare with instance_id");

    // The forwarded proposal should have been learned
    let leader_learned = nodes[0].get_commit_id(forwarded_instance);
    assert!(
        leader_learned.is_some(),
        "Leader should have committed the forwarded value (instance {})",
        forwarded_instance
    );
    let follower_learned = nodes[1].get_commit_id(forwarded_instance);
    assert!(
        follower_learned.is_some(),
        "Follower should have learned the value via Learn broadcast (instance {})",
        forwarded_instance
    );
}

/// Leader propose() still works directly (not forwarded).
#[test]
fn test_leader_propose_still_direct() {
    let mut nodes: Vec<MultiPaxosNode<MySM>> = (0..3u32)
        .map(|id| MultiPaxosNode::new(make_config(id)))
        .collect();

    // Node 0 wins a round → becomes leader
    run_full_round(&mut nodes, 0, Command::Set(1));
    assert!(nodes[0].leader_ballot().is_some());

    // Leader proposes again → should produce Prepare directly, not RequestCommandToLeader
    let (msgs, _rx) = nodes[0].propose(Command::Set(2)).unwrap();
    let has_prepare = msgs
        .iter()
        .any(|m| matches!(m.clone().kind(), MessageKind::PrepareMsg(_)));
    assert!(
        has_prepare,
        "Leader should produce Prepare directly when proposing"
    );
}

/// Leader receives two proposals back-to-back — each gets a distinct instance.
#[test]
fn test_leader_sequential_proposals_get_distinct_instances() {
    let mut nodes: Vec<MultiPaxosNode<MySM>> = (0..3u32)
        .map(|id| MultiPaxosNode::new(make_config(id)))
        .collect();

    run_full_round(&mut nodes, 0, Command::Set(1));
    assert!(nodes[0].leader_ballot().is_some());

    // Two proposals on the leader
    let (msgs_a, _rx_a) = nodes[0].propose(Command::Set(10)).unwrap();
    let (msgs_b, _rx_b) = nodes[0].propose(Command::Set(20)).unwrap();

    // Extract instance IDs from the Prepare messages
    let instance_a = msgs_a
        .iter()
        .find_map(|m| match m.clone().kind() {
            MessageKind::PrepareMsg(_) => Some(m.instance_id()),
            _ => None,
        })
        .unwrap();
    let instance_b = msgs_b
        .iter()
        .find_map(|m| match m.clone().kind() {
            MessageKind::PrepareMsg(_) => Some(m.instance_id()),
            _ => None,
        })
        .unwrap();

    assert_ne!(
        instance_a, instance_b,
        "Sequential proposals must get different instance IDs"
    );
}

/// Two non-leaders forward to leader — leader assigns distinct instances for each.
#[test]
fn test_two_forwarded_proposals_get_distinct_instances() {
    let mut nodes: Vec<MultiPaxosNode<MySM>> = (0..3u32)
        .map(|id| MultiPaxosNode::new(make_config(id)))
        .collect();

    run_full_round(&mut nodes, 0, Command::Set(1));
    assert!(nodes[0].leader_ballot().is_some());

    // Node 1 and Node 2 both forward proposals
    let (fwd1, _rx1) = nodes[1].propose(Command::Set(10)).unwrap();
    let (fwd2, _rx2) = nodes[2].propose(Command::Set(20)).unwrap();

    // Leader handles both forwarded proposals
    let leader_msgs_a = deliver_to(&mut nodes[0], &fwd1);
    let leader_msgs_b = deliver_to(&mut nodes[0], &fwd2);

    let instance_a = leader_msgs_a
        .iter()
        .find_map(|m| match m.clone().kind() {
            MessageKind::PrepareMsg(_) => Some(m.instance_id()),
            _ => None,
        })
        .unwrap();
    let instance_b = leader_msgs_b
        .iter()
        .find_map(|m| match m.clone().kind() {
            MessageKind::PrepareMsg(_) => Some(m.instance_id()),
            _ => None,
        })
        .unwrap();

    assert_ne!(
        instance_a, instance_b,
        "Forwarded proposals must get different instance IDs"
    );
}
