pub use synod::*;

use crate::Channel;

mod synod;

type Ballot = u32;

// This is the value that proposer is going to send. It should be a Command c.
type Value = u32;

/// A paxos node is a process that participates in a paxos consensus algorithm.
/// It's the main entry to the paxos algorithm.
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
