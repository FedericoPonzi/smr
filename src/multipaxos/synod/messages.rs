use std::fmt::{Debug, Formatter};

use crate::multipaxos::{Ballot, MaxAcceptedProposal};
use crate::CommandTrait;

pub type SenderId = u32;

pub struct Message<C>
where
    C: CommandTrait,
{
    // TODO: remove sender_id from inside the message kinds.
    sender_id: SenderId,
    msg: MessageKind<C>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MessageKind<C: CommandTrait> {
    RequestCommandToLeader(C),
    PrepareMsg(Prepare),
    PromiseMsg(Promise<C>),
    AcceptMsg(Accept<C>),
    AckAcceptMsg(AckAccept),
    LearnMsg(Learn<C>),
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Prepare {
    // Not needed, keep around for now just todebug:
    pub sender: SenderId,
    pub(crate) ballot: Ballot,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Promise<C>
where
    C: CommandTrait,
{
    pub sender: SenderId,
    pub(crate) max_accepted: Option<MaxAcceptedProposal<C>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Accept<C: CommandTrait> {
    pub sender: SenderId,
    pub(crate) ballot: Ballot,
    pub(crate) command: C,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AckAccept {
    pub sender: SenderId,
    pub(crate) ballot: Ballot,
}

#[derive(Clone, PartialEq, Eq, Hash)]
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
