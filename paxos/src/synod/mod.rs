mod acceptor;
mod learner;
mod messages;
mod proposer;

use crate::{Ballot, Value};
pub use acceptor::Acceptor;
pub use learner::Learner;
pub use messages::*;
pub use proposer::Proposer;

#[derive(Debug, Clone, Ord, Eq, PartialOrd, PartialEq, Hash)]
pub struct MaxAcceptedProposal(pub Ballot, pub Value);
