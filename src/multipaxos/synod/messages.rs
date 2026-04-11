use crate::CommandTrait;
use crate::multipaxos::{Ballot, MaxAcceptedProposal};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};

pub type SenderId = u32;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Message<T>
where
    T: CommandTrait,
{
    // TODO: remove sender_id from inside the message kinds.
    sender_id: SenderId,
    paxos_instance: u64,
    msg: MessageKind<T>,
}
impl<T> Message<T>
where
    T: CommandTrait,
{
    pub fn new(sender_id: SenderId, msg: MessageKind<T>, paxos_instance: u64) -> Self {
        Message {
            sender_id,
            msg,
            paxos_instance,
        }
    }
    pub fn sender_id(&self) -> SenderId {
        self.sender_id
    }
    pub fn instance_id(&self) -> u64 {
        self.paxos_instance
    }
    pub fn kind(self) -> MessageKind<T> {
        self.msg
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MessageKind<T>
where
    T: CommandTrait,
{
    RequestCommandToLeader { cmd: T, forward_id: u64 },
    ForwardAck { forward_id: u64, instance_id: u64 },
    PrepareMsg(Prepare),
    PromiseMsg(Promise<T>),
    AcceptMsg(Accept<T>),
    AckAcceptMsg(AckAccept),
    NackPrepareMsg(NackPrepare),
    NackAcceptMsg(NackAccept),
    LearnMsg(Learn<T>),
    LearnedCommand { cmd: T },
}

impl<C> From<Prepare> for MessageKind<C>
where
    C: CommandTrait,
{
    fn from(proposal: Prepare) -> Self {
        MessageKind::PrepareMsg(proposal)
    }
}

impl<C> From<Promise<C>> for MessageKind<C>
where
    C: CommandTrait,
{
    fn from(promise: Promise<C>) -> Self {
        MessageKind::PromiseMsg(promise)
    }
}

impl<C> From<Accept<C>> for MessageKind<C>
where
    C: CommandTrait,
{
    fn from(accept: Accept<C>) -> Self {
        MessageKind::AcceptMsg(accept)
    }
}

impl<C: CommandTrait> From<AckAccept> for MessageKind<C> {
    fn from(accepted: AckAccept) -> Self {
        MessageKind::AckAcceptMsg(accepted)
    }
}

impl<C: CommandTrait> From<Learn<C>> for MessageKind<C> {
    fn from(accepted: Learn<C>) -> Self {
        MessageKind::LearnMsg(accepted)
    }
}

impl<C: CommandTrait> From<NackPrepare> for MessageKind<C> {
    fn from(nack: NackPrepare) -> Self {
        MessageKind::NackPrepareMsg(nack)
    }
}

impl<C: CommandTrait> From<NackAccept> for MessageKind<C> {
    fn from(nack: NackAccept) -> Self {
        MessageKind::NackAcceptMsg(nack)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Prepare {
    // Not needed, keep around for now just todebug:
    pub sender: SenderId,
    pub(crate) ballot: Ballot,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Promise<C>
where
    C: CommandTrait,
{
    pub sender: SenderId,
    pub(crate) ballot: Ballot,
    pub(crate) max_accepted: Option<MaxAcceptedProposal<C>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Accept<C: CommandTrait> {
    pub sender: SenderId,
    pub(crate) ballot: Ballot,
    pub(crate) command: C,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AckAccept {
    pub sender: SenderId,
    pub(crate) ballot: Ballot,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NackPrepare {
    pub sender: SenderId,
    pub(crate) max_ballot: Ballot,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NackAccept {
    pub sender: SenderId,
    pub(crate) max_ballot: Ballot,
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Learn<C: CommandTrait> {
    pub(crate) sender: SenderId,
    pub(crate) ballot: Ballot,
    pub(crate) command: C,
}
impl<C> Debug for Learn<C>
where
    C: CommandTrait,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Learn")
            .field("sender", &self.sender)
            .field("ballot", &self.ballot)
            .field("command", &self.command)
            .finish()
    }
}
