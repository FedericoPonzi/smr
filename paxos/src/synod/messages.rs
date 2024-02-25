use crate::synod::MaxAcceptedProposal;
use crate::{Ballot, Value};

pub struct Proposal {
    pub(crate) ballot: Ballot,
}

pub struct Promise {
    pub(crate) max_accepted: Option<MaxAcceptedProposal>,
}

pub struct Accept {
    pub(crate) ballot: Ballot,
    pub(crate) value: Value,
}

pub struct Accepted {
    pub(crate) ballot: Ballot,
    pub(crate) value: Value,
}
