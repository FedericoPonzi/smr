use crate::multipaxos::{Ballot, MaxAcceptedProposal, Value};

pub type SenderId = u32;

// TODO: probably Message could be enriched with a destination id. Like Proposers, Acceptors, or Leader.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Message {
    PrepareMsg(Prepare),
    PromiseMsg(Promise),
    AcceptMsg(Accept),
    AckAcceptMsg(AckAccept),
    LearnMsg(Learn),
}

impl From<Prepare> for Message {
    fn from(proposal: Prepare) -> Self {
        Message::PrepareMsg(proposal)
    }
}

impl From<Promise> for Message {
    fn from(promise: Promise) -> Self {
        Message::PromiseMsg(promise)
    }
}

impl From<Accept> for Message {
    fn from(accept: Accept) -> Self {
        Message::AcceptMsg(accept)
    }
}

impl From<AckAccept> for Message {
    fn from(accepted: AckAccept) -> Self {
        Message::AckAcceptMsg(accepted)
    }
}

impl From<Learn> for Message {
    fn from(accepted: Learn) -> Self {
        Message::LearnMsg(accepted)
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
