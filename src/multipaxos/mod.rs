use std::collections::HashMap;
pub use synod::*;

use crate::{
    Channel, Result, SerializableCommand, SmrConfig, StateMachine, StateMachineReplicationAlgorithm,
};

mod synod;

type Ballot = u32;

/// A paxos node is a process that participates in a multipaxos consensus algorithm.
/// It's the main entry to the paxos algorithm.
pub struct MultiPaxosNode<T>
where
    T: SerializableCommand,
{
    id: u32,
    config: SmrConfig,
    paxos_instances: HashMap<u64, PaxosInstance<T>>,
    next_instance_id: u64,
}

impl<T> MultiPaxosNode<T>
where
    T: SerializableCommand,
{
    pub fn new(id: u32, config: SmrConfig) -> Self {
        Self {
            id,
            config,
            paxos_instances: HashMap::new(),
            next_instance_id: 0,
        }
    }
}

impl<T> StateMachineReplicationAlgorithm<T> for MultiPaxosNode<T>
where
    T: SerializableCommand,
{
    type SMRMessage = Message<T>;
    // actively push a value.

    fn propose(&mut self, cmd: T) -> Result<Vec<Message<T>>> {
        let instance_id = self.next_instance_id;
        self.next_instance_id += 1;

        let mut instance = PaxosInstance::new(self.id, self.config.total_nodes / 2 + 1);
        let prepare = instance.proposer.new_prepare(cmd.clone());
        let mut outgoing_messages = Vec::new();
        self.paxos_instances.insert(instance_id, instance);
        outgoing_messages.push(Message::new(self.id, prepare, instance_id));
        Ok(outgoing_messages)
    }

    // todo: partecipate in rounds
    fn handle_message(&mut self, paxos_msg: Message<T>) -> Result<Vec<Message<T>>> {
        let mut outgoing_messages = Vec::new();

        let instance_id = paxos_msg.instance_id();

        if let Some(instance) = self.paxos_instances.get_mut(&instance_id) {
            if let Some(response) = instance.handle_message(paxos_msg.kind())? {
                outgoing_messages.push(Message::new(self.id, response, instance_id));
            }
        } else {
            let mut instance = PaxosInstance::new(self.id, self.config.total_nodes / 2 + 1);
            if let Some(response) = instance.handle_message(paxos_msg.kind())? {
                outgoing_messages.push(Message::new(self.id, response, instance_id));
            }
            self.paxos_instances.insert(instance_id, instance);
        }

        Ok(outgoing_messages)
    }
}
