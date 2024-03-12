use std::collections::HashSet;

use crate::multipaxos::{
    Accept, AckAccept, Ballot, Learn, MaxAcceptedProposal, Prepare, Promise, Value,
};

#[derive(Default, Debug, Clone)]
struct ProposerBallotState {
    // number of acceptors that promised to participate to in this ballot
    promises: u32,
    max_accepted_proposals: HashSet<MaxAcceptedProposal>,
    // number of acceptors that accepted this ballot
    accepts: u32,
}

#[derive(Debug, Clone)]
pub struct Proposer {
    ballot: Ballot,
    proposer_id: u32,
    ballot_state: ProposerBallotState,
    quorum_size: u32,
    value: Value,
}

impl Proposer {
    pub fn new(proposer_id: u32, quorum_size: u32, value: Value) -> Self {
        Self {
            proposer_id,
            quorum_size,
            ballot: proposer_id,
            ballot_state: Default::default(),
            value,
        }
    }

    pub(crate) fn new_prepare(&mut self) -> Prepare {
        self.ballot += self.proposer_id;
        Prepare {
            sender: self.proposer_id,
            ballot: self.ballot,
        }
    }

    pub fn handle_promise(&mut self, response: Promise) -> Option<Accept> {
        if response.max_accepted.is_some() {
            self.ballot_state
                .max_accepted_proposals
                .insert(response.max_accepted.unwrap());
        }
        self.ballot_state.promises += 1;
        if self.ballot_state.promises >= self.quorum_size {
            Some(Accept {
                sender: self.proposer_id,
                ballot: self.ballot,
                value: self.value,
            })
        } else {
            None
        }
    }
    pub fn handle_ack_accept(&mut self, ack_accept: AckAccept) -> Option<Learn> {
        // old message, maybe due to a crash restart
        if ack_accept.ballot != self.ballot {
            return None;
        }
        self.ballot_state.accepts += 1;
        if self.ballot_state.accepts >= self.quorum_size {
            Some(Learn {
                sender: self.proposer_id,
                ballot: self.ballot,
                value: self.value,
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proposer() {
        let mut proposer = Proposer::new(1, 3, 123);
        let proposal = proposer.new_prepare();
        let mut promise = Promise {
            sender: 2,
            max_accepted: None,
        };
        proposer.handle_promise(promise.clone());
        assert_eq!(proposer.ballot_state.promises, 1);
        assert_eq!(proposer.ballot_state.max_accepted_proposals.len(), 0);
        assert_eq!(proposer.ballot_state.accepts, 0);
    }
}
