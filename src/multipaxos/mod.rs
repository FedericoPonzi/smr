pub use synod::*;

use crate::{Channel, CommandTrait, StateMachine, StateMachineReplication};

mod synod;

type Ballot = u32;

/// A paxos node is a process that participates in a multipaxos consensus algorithm.
/// It's the main entry to the paxos algorithm.
pub struct MultiPaxosNode<T, C>
where
    T: Channel<C>,
    C: CommandTrait,
{
    id: u32,
    channel: T,
    state: InnerState<C>,
}

struct InnerState<C> {
    round: Vec<C>,
}

impl<T, C> MultiPaxosNode<T, C>
where
    T: Channel<C>,
    C: CommandTrait,
{
    pub fn new(id: u32, channel: T) -> Self {
        Self {
            id,
            channel,
            state: InnerState { round: Vec::new() },
        }
    }
}

impl<SM, T, C> StateMachineReplication<SM> for MultiPaxosNode<T, C>
where
    T: Channel<C>,
    SM: StateMachine,
    C: CommandTrait,
{
    fn propose(&mut self, cmd: SM::Command) -> anyhow::Result<SM::State> {
        todo!()
    }
}
