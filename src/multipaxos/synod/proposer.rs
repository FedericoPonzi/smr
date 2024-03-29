use std::collections::HashSet;
use std::fmt::Debug;

use crate::multipaxos::{
    Accept, AckAccept, Ballot, Learn, MaxAcceptedProposal, MessageKind, Prepare, Promise, Value,
};

#[derive(Debug, Clone)]
struct InitialState {}

#[derive(Debug, Clone)]
struct ProposalState {
    promises: u32,
    max_accepted_proposals: HashSet<MaxAcceptedProposal>,
    value: Value,
}

#[derive(Debug, Clone)]
struct AcceptingState {
    // number of acceptors that accepted this ballot
    accepts: u32,
    value: Value,
}

#[derive(Debug, Clone)]
struct StateWrapper<S> {
    state: S,
    ballot: Ballot,
    proposer_id: u32,
    quorum_size: u32,
}
impl<S> StateWrapper<S> {
    pub(crate) fn new_prepare(&mut self, value: Value) -> (Option<MessageKind>, InnerState) {
        self.ballot += self.proposer_id;
        (
            Some(MessageKind::PrepareMsg(Prepare {
                sender: self.proposer_id,
                ballot: self.ballot,
            })),
            InnerState::ProposalInnerState(StateWrapper {
                state: ProposalState {
                    promises: 0,
                    max_accepted_proposals: HashSet::new(),
                    value,
                },
                ballot: self.ballot,
                proposer_id: self.proposer_id,
                quorum_size: self.quorum_size,
            }),
        )
    }
}

impl StateWrapper<ProposalState> {
    pub fn handle_promise(&mut self, response: Promise) -> (Option<Accept>, InnerState) {
        if response.max_accepted.is_some() {
            self.state
                .max_accepted_proposals
                .insert(response.max_accepted.unwrap());
        }
        self.state.promises += 1;
        if self.state.promises >= self.quorum_size {
            println!("Proposer has reached the quorum of promises.");
            (
                Some(Accept {
                    sender: self.proposer_id,
                    ballot: self.ballot,
                    value: self.state.value,
                }),
                InnerState::AcceptingState(StateWrapper {
                    state: AcceptingState {
                        accepts: 0,
                        value: self.state.value,
                    },
                    ballot: self.ballot,
                    proposer_id: self.proposer_id,
                    quorum_size: self.quorum_size,
                }),
            )
        } else {
            (None, InnerState::ProposalInnerState(self.clone()))
        }
    }
}

impl StateWrapper<AcceptingState> {
    pub fn handle_ack_accept(&mut self, ack_accept: AckAccept) -> (Option<Learn>, InnerState) {
        // old message, maybe due to a crash restart
        if ack_accept.ballot != self.ballot {
            return (None, InnerState::AcceptingState(self.clone()));
        }
        self.state.accepts += 1;
        if self.state.accepts >= self.quorum_size {
            (
                Some(Learn {
                    sender: self.proposer_id,
                    ballot: self.ballot,
                    value: self.state.value,
                }),
                InnerState::LearningState(StateWrapper {
                    state: InitialState {},
                    ballot: self.ballot,
                    proposer_id: self.proposer_id,
                    quorum_size: self.quorum_size,
                }),
            )
        } else {
            (None, InnerState::AcceptingState(self.clone()))
        }
    }
}

#[derive(Debug, Clone)]
enum InnerState {
    InitialInnerState(StateWrapper<InitialState>),
    ProposalInnerState(StateWrapper<ProposalState>),
    AcceptingState(StateWrapper<AcceptingState>),
    LearningState(StateWrapper<InitialState>),
}
fn wrap_message<T: Into<MessageKind> + Debug>(
    m: (Option<T>, InnerState),
) -> (Option<MessageKind>, InnerState) {
    println!("{:?}", m);
    (m.0.map(Into::into), m.1)
}

impl InnerState {
    pub fn new_prepare(&mut self, value: Value) -> (Option<MessageKind>, InnerState) {
        match self {
            InnerState::InitialInnerState(state_wrapper) => state_wrapper.new_prepare(value),
            InnerState::ProposalInnerState(state_wrapper) => state_wrapper.new_prepare(value),
            InnerState::AcceptingState(state_wrapper) => state_wrapper.new_prepare(value),
            InnerState::LearningState(state_wrapper) => state_wrapper.new_prepare(value),
        }
    }
    pub fn handle_message(self, message: MessageKind) -> (Option<MessageKind>, InnerState) {
        match (self, message) {
            (
                InnerState::ProposalInnerState(mut state_wrapper),
                MessageKind::PromiseMsg(promise),
            ) => wrap_message(state_wrapper.handle_promise(promise)),
            (
                InnerState::AcceptingState(mut state_wrapper),
                MessageKind::AckAcceptMsg(ack_accept),
            ) => wrap_message(state_wrapper.handle_ack_accept(ack_accept)),
            r => (None, r.0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Proposer {
    inner_state: InnerState,
}

impl Proposer {
    pub fn new(proposer_id: u32, quorum_size: u32, value: Value) -> Self {
        Self {
            inner_state: InnerState::InitialInnerState(StateWrapper {
                state: InitialState {},
                ballot: proposer_id,
                proposer_id,
                quorum_size,
            }),
        }
    }
    pub fn new_prepare(&mut self, value: Value) -> MessageKind {
        let (msg, new_state) = self.inner_state.new_prepare(value);
        self.inner_state = new_state;
        msg.unwrap()
    }
    pub fn handle_message<T: Into<MessageKind>>(&mut self, message: T) -> Option<MessageKind> {
        let (message, new_state) = self.inner_state.clone().handle_message(message.into());
        println!("message: {:?}", message);
        self.inner_state = new_state;
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proposer() {
        let mut proposer = Proposer::new(1, 2, 123);
        let mut prepare = proposer.new_prepare(123);
        let mut promise = Promise {
            sender: 2,
            max_accepted: None,
        };
        let resp = proposer.handle_message(promise.clone());
        assert!(resp.is_none());
        let mut ballot = 0;
        if let InnerState::ProposalInnerState(state_wrapper) = &proposer.inner_state {
            assert_eq!(state_wrapper.state.promises, 1);
            assert!(state_wrapper.state.max_accepted_proposals.is_empty());
            assert_eq!(state_wrapper.state.value, 123);
            ballot = state_wrapper.ballot
        } else {
            panic!("Failed");
        }
        let resp = proposer.handle_message(Promise {
            sender: 3,
            max_accepted: None,
        });
        assert!(matches!(resp.unwrap(), MessageKind::AcceptMsg(_)));
        assert!(matches!(
            proposer.inner_state,
            InnerState::AcceptingState(_)
        ));
        let resp = proposer.handle_message(AckAccept { sender: 2, ballot });
        assert!(resp.is_none());
        assert!(matches!(
            proposer.inner_state,
            InnerState::AcceptingState(_)
        ));
        let resp = proposer.handle_message(AckAccept { sender: 3, ballot });
        assert!(resp.is_some_and(|t| matches!(t, MessageKind::LearnMsg(_))));
        assert!(matches!(proposer.inner_state, InnerState::LearningState(_)));
    }
}
