/**
* A leader process has a unique identifier called the leader identifier. Identifiers are totally ordered.
* A ballot has a unique identifier as well, called its ballot number. Ballot numbers are totally ordered.
* ballot numbers be lexicographically ordered pairs of an integer and its leader identifier (consequently, leader identifiers need to be totally ordered as well).
* This way, given a ballot number, it is trivial to see who the leader of the ballot is.
*/
mod channel;
mod synod;

pub use channel::*;
pub use synod::*;

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
