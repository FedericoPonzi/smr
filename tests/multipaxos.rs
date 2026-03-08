use serde::{Deserialize, Serialize};
use smr::multipaxos::MultiPaxosNode;
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
