use crate::multipaxos::{Ballot, MaxAcceptedProposal, Value};

pub type SenderId = u32;

pub struct Message {
    // TODO: remove sender_id from inside the message kinds.
    sender_id: SenderId,
    msg: MessageKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MessageKind {
    PrepareMsg(Prepare),
    PromiseMsg(Promise),
    AcceptMsg(Accept),
    AckAcceptMsg(AckAccept),
    LearnMsg(Learn),
}

impl From<Prepare> for MessageKind {
    fn from(proposal: Prepare) -> Self {
        MessageKind::PrepareMsg(proposal)
    }
}

impl From<Promise> for MessageKind {
    fn from(promise: Promise) -> Self {
        MessageKind::PromiseMsg(promise)
    }
}

impl From<Accept> for MessageKind {
    fn from(accept: Accept) -> Self {
        MessageKind::AcceptMsg(accept)
    }
}

impl From<AckAccept> for MessageKind {
    fn from(accepted: AckAccept) -> Self {
        MessageKind::AckAcceptMsg(accepted)
    }
}

impl From<Learn> for MessageKind {
    fn from(accepted: Learn) -> Self {
        MessageKind::LearnMsg(accepted)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Prepare {
    // Not needed, keep around for now just todebug:
    pub sender: SenderId,
    pub(crate) ballot: Ballot,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Promise {
    pub sender: SenderId,
    pub(crate) max_accepted: Option<MaxAcceptedProposal>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Accept {
    pub sender: SenderId,
    pub(crate) ballot: Ballot,
    pub(crate) value: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AckAccept {
    pub sender: SenderId,
    pub(crate) ballot: Ballot,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Learn {
    pub(crate) sender: SenderId,
    pub(crate) ballot: Ballot,
    pub(crate) value: Value,
}
