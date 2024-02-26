mod acceptor;
mod learner;
mod messages;
mod proposer;

pub use acceptor::Acceptor;
pub use learner::Learner;
pub use messages::*;
pub use proposer::Proposer;
use crate::multipaxos::{Ballot, Value};

#[derive(Debug, Clone, Ord, Eq, PartialOrd, PartialEq, Hash)]
pub struct MaxAcceptedProposal(pub Ballot, pub Value);
