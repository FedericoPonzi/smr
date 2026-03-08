use log::info;
use std::collections::HashMap;
pub use synod::*;
use tokio::sync::oneshot;

use crate::{
    CommitResult, ProposalResult, Result, SmrConfig, StateMachine, StateMachineReplicationAlgorithm,
};
use storage::PaxosStorage;

pub mod storage;
mod synod;
pub(crate) mod trace;

pub(crate) type Ballot = u32;

/// A paxos node is a process that participates in a multipaxos consensus algorithm.
/// It's the main entry to the paxos algorithm.
pub struct MultiPaxosNode<S>
where
    S: StateMachine,
{
    config: SmrConfig,
    paxos_instances: HashMap<u64, PaxosInstance<S::Command>>,
    pending_senders: HashMap<u64, oneshot::Sender<S::Output>>,
    next_instance_id: u64,
    id: u32,
    storage: Option<PaxosStorage<S::Command>>,
    /// Ballot of the last round this node won. `Some` means we are the leader.
    leader_ballot: Option<Ballot>,
    /// Node ID of the known leader (set when we see them win a round).
    known_leader_id: Option<u32>,
    /// Senders for proposals forwarded to the leader, keyed by command.
    forwarded_senders: Vec<(S::Command, oneshot::Sender<S::Output>)>,
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
            storage: None,
            leader_ballot: None,
            known_leader_id: None,
            forwarded_senders: Vec::new(),
        }
    }

    pub fn with_storage(config: SmrConfig, storage: PaxosStorage<S::Command>) -> Result<Self> {
        let id = config.node_id;
        let quorum = config.total_nodes / 2 + 1;
        let total = config.total_nodes;

        let mut paxos_instances = HashMap::new();
        let mut next_instance_id = 0u64;

        // Restore acceptor state from storage
        for (instance_id, state) in storage.load_all_acceptor_states()? {
            let mut instance = PaxosInstance::new(id, quorum, total, instance_id);
            instance.restore_acceptor(&state);
            paxos_instances.insert(instance_id, instance);
            if instance_id >= next_instance_id {
                next_instance_id = instance_id + 1;
            }
        }

        Ok(Self {
            id,
            config,
            paxos_instances,
            pending_senders: HashMap::new(),
            next_instance_id,
            storage: Some(storage),
            leader_ballot: None,
            known_leader_id: None,
            forwarded_senders: Vec::new(),
        })
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    /// Returns the current leader ballot, if this node is the leader.
    pub fn leader_ballot(&self) -> Option<Ballot> {
        self.leader_ballot
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
            if let Some(response) = instance.handle_message(msg, self.storage.as_mut())? {
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
    fn propose(&mut self, cmd: S::Command) -> Result<ProposalResult<S>> {
        // Forward to leader if we know someone else is leading
        if self.leader_ballot.is_none() && self.known_leader_id.is_some() {
            info!(
                "Node {}: not leader, forwarding proposal to node {:?}",
                self.id, self.known_leader_id
            );
            let (sender, receiver) = oneshot::channel();
            let forward_msg = Message::new(
                self.id,
                MessageKind::RequestCommandToLeader(cmd.clone()),
                0, // instance_id irrelevant — leader assigns it
            );
            self.forwarded_senders.push((cmd, sender));
            return Ok((vec![forward_msg], receiver));
        }

        let instance_id = self.next_instance_id;
        self.next_instance_id += 1;

        info!(
            "Node {}: proposing for instance {} cmd={:?}",
            self.id, instance_id, cmd
        );

        let mut instance = PaxosInstance::new(
            self.id,
            self.config.total_nodes / 2 + 1,
            self.config.total_nodes,
            instance_id,
        );
        let prepare = instance.proposer.new_prepare(cmd.clone());
        if let MessageKind::PrepareMsg(ref prep) = prepare {
            trace::trace_phase1a(self.id, instance_id, prep.ballot);
        }
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
        // Intercept forwarded proposals at this level — leader assigns instance_id.
        if let MessageKind::RequestCommandToLeader(cmd) = paxos_msg.clone().kind() {
            if self.leader_ballot.is_some() {
                info!(
                    "Node {}: handling forwarded proposal cmd={:?}",
                    self.id, cmd
                );
                let (msgs, _rx) = self.propose(cmd)?;
                return Ok(msgs);
            }
            // Not leader either — drop or re-forward (drop for now)
            info!(
                "Node {}: received forwarded proposal but not leader, dropping",
                self.id
            );
            return Ok(vec![]);
        }

        let mut outgoing_messages = Vec::new();

        let instance_id = paxos_msg.instance_id();
        let msg_kind = paxos_msg.kind();
        let is_incoming_learn = matches!(&msg_kind, MessageKind::LearnMsg(_));
        let incoming_learn_sender = match &msg_kind {
            MessageKind::LearnMsg(learn) => Some(learn.sender),
            _ => None,
        };

        info!(
            "Node {}: received network message for instance {}",
            self.id, instance_id
        );
        // Keep next_instance_id ahead of any instance we've seen from other nodes
        if instance_id >= self.next_instance_id {
            self.next_instance_id = instance_id + 1;
        }
        self.paxos_instances.entry(instance_id).or_insert_with(|| {
            PaxosInstance::new(
                self.id,
                self.config.total_nodes / 2 + 1,
                self.config.total_nodes,
                instance_id,
            )
        });

        let response = {
            let paxos_instance = self.paxos_instances.get_mut(&instance_id).unwrap();
            paxos_instance.handle_message(msg_kind, self.storage.as_mut())?
        };

        if let Some(ref response) = response {
            // Our proposer produced a Learn (from AckAccept quorum) → we won this round
            if !is_incoming_learn && let MessageKind::LearnMsg(learn) = response {
                info!(
                    "Node {}: won round for instance {} with ballot {}",
                    self.id, instance_id, learn.ballot
                );
                self.leader_ballot = Some(learn.ballot);
                self.known_leader_id = None; // we are the leader
            }
            // We promised a higher ballot → our old leader ballot is no longer valid
            if let MessageKind::PromiseMsg(promise) = response
                && let Some(lb) = self.leader_ballot
                && promise.ballot > lb
            {
                info!(
                    "Node {}: lost leadership (promised ballot {} > leader ballot {})",
                    self.id, promise.ballot, lb
                );
                self.leader_ballot = None;
            }
        }

        // Track known leader from incoming Learn messages from other nodes
        if let Some(proposer_id) = incoming_learn_sender
            && proposer_id != self.id
        {
            self.known_leader_id = Some(proposer_id);
        }

        if let Some(response) = response {
            outgoing_messages.push(Message::new(self.id, response.clone(), instance_id));
            self.self_deliver(instance_id, response, &mut outgoing_messages)?;
        }
        Ok(outgoing_messages)
    }

    fn get_commit_id(&mut self, id: u64) -> Option<CommitResult<S>> {
        let instance = self.paxos_instances.get(&id)?;
        let command = instance.get_value()?;
        let sender = self.pending_senders.remove(&id).or_else(|| {
            // Match forwarded proposal by command equality
            let pos = self
                .forwarded_senders
                .iter()
                .position(|(cmd, _)| *cmd == command)?;
            Some(self.forwarded_senders.remove(pos).1)
        });
        Some((command, sender))
    }
}
