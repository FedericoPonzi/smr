use serde::{Deserialize, Serialize};
use smr::multipaxos::MultiPaxosNode;
use smr::{StateMachine, StateMachineReplicationAlgorithm};
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
#[test]
fn test_smr_multipaxos() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    let other_nodes = vec!["123".into(), "124".into(), "125".into()];
    let mut mpn1: MultiPaxosNode<MySM> = smr::multipaxos::MultiPaxosNode::new(
        smr::SmrConfig::new(1, None, other_nodes.clone()).unwrap(),
    );
    let mpn2: MultiPaxosNode<MySM> =
        smr::multipaxos::MultiPaxosNode::new(smr::SmrConfig::new(2, None, other_nodes).unwrap());
    let out = mpn1.propose(Command::Set(42)).unwrap();
    let out = mpn1.propose(Command::Set(42)).unwrap();
    let out = mpn1.propose(Command::Set(42)).unwrap();
    let out = mpn1.propose(Command::Set(42)).unwrap();
    debug!("{:?}", out.0);
}
