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
    pending_senders: HashMap<u64, oneshot::Sender<S::Output>>,
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
            pending_senders: HashMap::new(),
            next_instance_id: 0,
        }
    }

    /// Process a message locally through the PaxosInstance and cascade any responses.
    /// Each response is also added to outgoing messages for network broadcast.
    fn self_deliver(
        &mut self,
        instance_id: u64,
        initial_msg: MessageKind<S::Command>,
        outgoing: &mut Vec<Message<S::Command>>,
    ) -> Result<()> {
        let mut to_deliver = vec![initial_msg];
        while let Some(msg) = to_deliver.pop() {
            let instance = self.paxos_instances.get_mut(&instance_id).unwrap();
            if let Some(response) = instance.handle_message(msg)? {
                outgoing.push(Message::new(self.id, response.clone(), instance_id));
                to_deliver.push(response);
            }
        }
        Ok(())
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

        let mut instance = PaxosInstance::new(self.id, self.config.total_nodes / 2 + 1, self.config.total_nodes);
        let prepare = instance.proposer.new_prepare(cmd.clone());
        self.paxos_instances.insert(instance_id, instance);

        let mut outgoing_messages = vec![Message::new(self.id, prepare.clone(), instance_id)];
        self.self_deliver(instance_id, prepare, &mut outgoing_messages)?;

        let (sender, receiver) = oneshot::channel();
        self.pending_senders.insert(instance_id, sender);
        Ok((outgoing_messages, receiver))
    }
    fn handle_message(
        &mut self,
        paxos_msg: Message<S::Command>,
    ) -> Result<Vec<Message<S::Command>>> {
        let mut outgoing_messages = Vec::new();

        let instance_id = paxos_msg.instance_id();
        self.paxos_instances
            .entry(instance_id)
            .or_insert(PaxosInstance::new(self.id, self.config.total_nodes / 2 + 1, self.config.total_nodes));

        let response = {
            let paxos_instance = self.paxos_instances.get_mut(&instance_id).unwrap();
            paxos_instance.handle_message(paxos_msg.kind())?
        };

        if let Some(response) = response {
            outgoing_messages.push(Message::new(self.id, response.clone(), instance_id));
            self.self_deliver(instance_id, response, &mut outgoing_messages)?;
        }
        Ok(outgoing_messages)
    }

    // actively push a value.
    fn get_commit_id(&mut self, id: u64) -> Option<(S::Command, Option<oneshot::Sender<S::Output>>)> {
        let instance = self.paxos_instances.get(&id)?;
        let command = instance.get_value()?;
        let sender = self.pending_senders.remove(&id);
        Some((command, sender))
    }
}
