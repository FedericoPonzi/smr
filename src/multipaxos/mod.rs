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
    /// Ballot from the last round this node won. Used as the credential for
    /// fast-path proposals: `new_accept(ballot, cmd)` reuses this ballot across
    /// new instances so acceptors accept via implicit promise without Phase 1.
    leader_ballot: Option<Ballot>,
    /// Node ID of the known leader (set when we see them win a round).
    known_leader_id: Option<u32>,
    /// Senders for proposals forwarded to the leader, keyed by forward_id.
    forwarded_senders: HashMap<u64, oneshot::Sender<S::Output>>,
    /// Monotonic counter for unique forward IDs.
    next_forward_id: u64,
    /// On the leader: maps instance_id → (forwarder_node_id, forward_id).
    forwarded_instances: HashMap<u64, (u32, u64)>,
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
            forwarded_senders: HashMap::new(),
            next_forward_id: 0,
            forwarded_instances: HashMap::new(),
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
            forwarded_senders: HashMap::new(),
            next_forward_id: 0,
            forwarded_instances: HashMap::new(),
        })
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    /// Returns true if this node is the current leader.
    pub fn is_leader(&self) -> bool {
        self.leader_ballot.is_some()
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
        if !self.is_leader() && self.known_leader_id.is_some() {
            info!(
                "Node {}: not leader, forwarding proposal to node {:?}",
                self.id, self.known_leader_id
            );
            let (sender, receiver) = oneshot::channel();
            let forward_id = self.next_forward_id;
            self.next_forward_id += 1;
            let forward_msg = Message::new(
                self.id,
                MessageKind::RequestCommandToLeader { cmd, forward_id },
                0, // instance_id irrelevant — leader assigns it
            );
            self.forwarded_senders.insert(forward_id, sender);
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

        let first_msg = if let Some(ballot) = self.leader_ballot {
            // Fast path: skip Phase 1, go directly to Accept
            let accept = instance.proposer.new_accept(ballot, cmd.clone());
            if let MessageKind::AcceptMsg(ref a) = accept {
                trace::trace_phase2a(self.id, instance_id, a.ballot, &format!("{:?}", a.command));
            }
            accept
        } else {
            let prepare = instance.proposer.new_prepare(cmd.clone());
            if let MessageKind::PrepareMsg(ref prep) = prepare {
                trace::trace_phase1a(self.id, instance_id, prep.ballot);
            }
            prepare
        };
        self.paxos_instances.insert(instance_id, instance);

        let mut outgoing_messages = vec![Message::new(self.id, first_msg.clone(), instance_id)];
        self.self_deliver(instance_id, first_msg, &mut outgoing_messages)?;

        let (sender, receiver) = oneshot::channel();
        self.pending_senders.insert(instance_id, sender);
        Ok((outgoing_messages, receiver))
    }

    fn handle_message(
        &mut self,
        paxos_msg: Message<S::Command>,
    ) -> Result<Vec<Message<S::Command>>> {
        // Intercept forwarded proposals at this level — leader assigns instance_id.
        if let MessageKind::RequestCommandToLeader { cmd, forward_id } = paxos_msg.clone().kind() {
            if self.is_leader() {
                let forwarder_id = paxos_msg.sender_id();
                info!(
                    "Node {}: handling forwarded proposal cmd={:?} from node {}",
                    self.id, cmd, forwarder_id
                );
                let (mut msgs, _rx) = self.propose(cmd)?;
                // Track which instance this forwarded proposal maps to
                let instance_id = self.next_instance_id - 1; // propose() already incremented
                self.forwarded_instances
                    .insert(instance_id, (forwarder_id, forward_id));
                // Send ForwardAck so the forwarder can map forward_id → instance_id
                msgs.push(Message::new(
                    self.id,
                    MessageKind::ForwardAck {
                        forward_id,
                        instance_id,
                    },
                    instance_id,
                ));
                return Ok(msgs);
            }
            info!(
                "Node {}: received forwarded proposal but not leader, dropping",
                self.id
            );
            return Ok(vec![]);
        }

        // Handle ForwardAck: move sender from forwarded_senders to pending_senders
        if let MessageKind::ForwardAck {
            forward_id,
            instance_id,
        } = paxos_msg.clone().kind()
        {
            if let Some(sender) = self.forwarded_senders.remove(&forward_id) {
                info!(
                    "Node {}: ForwardAck received, mapping forward_id={} to instance={}",
                    self.id, forward_id, instance_id
                );
                self.pending_senders.insert(instance_id, sender);
            }
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
        let incoming_nack_accept_ballot = match &msg_kind {
            MessageKind::NackAcceptMsg(nack) => Some(nack.max_ballot),
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

        // Incoming NackAccept means someone has a higher ballot → our fast path is stale
        if let Some(nack_ballot) = incoming_nack_accept_ballot
            && let Some(lb) = self.leader_ballot
            && nack_ballot > lb
        {
            info!(
                "Node {}: lost leadership (NackAccept max_ballot {} > leader ballot {})",
                self.id, nack_ballot, lb
            );
            self.leader_ballot = None;
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
        let sender = self.pending_senders.remove(&id);
        Some((command, sender))
    }
}
