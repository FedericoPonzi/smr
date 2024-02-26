mod synod;

pub use synod::*;
use crate::Channel;

type Ballot = u32;

// This is the value that proposer is going to send. It can be a command c.
type Value = u32;

enum Message {
    Proposal(Proposal),
    ProposalResponse(Promise),
    Accept(Accept),
    Accepted(Accepted),
}

/// A paxos node is a process that participates in a paxos consensus algorithm.
/// it's the main entry to the paxos algorithm.
pub struct Node<T: Channel> {
    id: u32,
    channel: T,
    state: NodeState,
}
struct NodeState {
    round: Vec<Value>,
}

impl<T> Node<T>
where
    T: Channel,
{
    pub fn new(id: u32, channel: T) -> Self {
        Self {
            id,
            channel,
            state: NodeState { round: Vec::new() },
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn it_works() {
        assert_eq!(1 + 1, 2);
    }
}
