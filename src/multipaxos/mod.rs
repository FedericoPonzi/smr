use std::collections::HashMap;
pub use synod::*;
use tokio::sync::oneshot;

use crate::{
    Result, SerializableCommand, SmrConfig, StateMachine, StateMachineReplicationAlgorithm,
};

mod synod;

type Ballot = u32;

/// A paxos node is a process that participates in a multipaxos consensus algorithm.
/// It's the main entry to the paxos algorithm.
/// TODO: I need to know if a value could still be potentially included in the current round, or if we should start negotiating the next round.
/// TODO: actually it would be much easier to just use a leader and let them handle it
pub struct MultiPaxosNode<S>
where
    S: StateMachine,
{
    config: SmrConfig,
    paxos_instances: HashMap<u64, PaxosInstance<S::Command>>,
    next_instance_id: u64,
    id: u32,
}

impl<S> MultiPaxosNode<S>
where
    S: StateMachine,
{
    pub fn new(config: SmrConfig) -> Self {
        Self {
            id: config.node_id,
            config,
            paxos_instances: HashMap::new(),
            next_instance_id: 0,
        }
    }
}

impl<S> StateMachineReplicationAlgorithm<S> for MultiPaxosNode<S>
where
    S: StateMachine,
{
    type SMRMessage = Message<S::Command>;
    fn propose(
        &mut self,
        cmd: S::Command,
    ) -> Result<(Vec<Message<S::Command>>, oneshot::Receiver<S::Output>)> {
        let instance_id = self.next_instance_id;
        self.next_instance_id += 1;

        let mut instance = PaxosInstance::new(self.id, self.config.total_nodes / 2 + 1);
        let prepare = instance.proposer.new_prepare(cmd.clone());
        let mut outgoing_messages = Vec::new();
        self.paxos_instances.insert(instance_id, instance);
        outgoing_messages.push(Message::new(self.id, prepare, instance_id));
        let (sender, receiver) = oneshot::channel();
        Ok((outgoing_messages, receiver))
    }
    fn handle_message(
        &mut self,
        paxos_msg: Message<S::Command>,
    ) -> Result<Vec<Message<S::Command>>> {
        let mut outgoing_messages = Vec::new();

        let instance_id = paxos_msg.instance_id();
        let paxos_instance = self
            .paxos_instances
            .entry(instance_id)
            .or_insert(PaxosInstance::new(self.id, self.config.total_nodes / 2 + 1));

        if let Some(response) = paxos_instance.handle_message(paxos_msg.kind())? {
            outgoing_messages.push(Message::new(self.id, response, instance_id));
        }
        Ok(outgoing_messages)
    }

    // actively push a value.
    fn get_commit_id(&mut self, id: u64) -> Option<S::Command> {
        let instance = self.paxos_instances.get(&id)?;
        instance.get_value()
    }
}
