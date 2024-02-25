use crate::synod::messages::{Promise, Proposal};
use crate::synod::MaxAcceptedProposal;
use crate::{Ballot, Value};
use std::collections::HashSet;

#[derive(Default)]
struct ProposerBallotState {
    // number of acceptors that promised to participate to in this ballot
    promises: u32,
    max_accepted_proposals: HashSet<MaxAcceptedProposal>,
    // number of acceptors that accepted this ballot
    accepts: u32,
}
pub struct Proposer {
    ballot: Ballot,
    value: Value,
    proposer_id: u32,
    ballot_state: ProposerBallotState,
}
impl Proposer {
    pub fn new(proposer_id: u32, value: u32) -> Self {
        Self {
            proposer_id,
            value,
            ballot: proposer_id,
            ballot_state: Default::default(),
        }
    }

    fn new_proposal(&mut self) -> Proposal {
        self.ballot += self.proposer_id;
        Proposal {
            ballot: self.ballot,
        }
    }

    fn handle_proposal_response(&mut self, response: Promise) {
        self.ballot_state
            .max_accepted_proposals
            .insert(response.max_accepted.unwrap());
        self.ballot_state.promises += 1;
    }
}
