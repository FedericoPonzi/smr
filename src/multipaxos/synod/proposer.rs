use std::collections::HashSet;
use std::fmt::Debug;

use log::{debug, info};

use crate::CommandTrait;
use crate::multipaxos::{
    Accept, AckAccept, Ballot, Learn, MaxAcceptedProposal, MessageKind, Prepare, Promise,
};

#[derive(Debug, Clone)]
struct InitialState {}

#[derive(Debug, Clone)]
struct ProposalState<C>
where
    C: CommandTrait,
{
    promises: u32,
    nacks: u32,
    max_accepted_proposals: HashSet<MaxAcceptedProposal<C>>,
    value: C,
}

#[derive(Debug, Clone)]
struct AcceptingState<C>
where
    C: CommandTrait,
{
    // number of acceptors that accepted this ballot
    accepts: u32,
    nacks: u32,
    value: C,
}

#[derive(Debug, Clone)]
struct StateWrapper<S> {
    state: S,
    ballot: Ballot,
    proposer_id: u32,
    quorum_size: u32,
    total_nodes: u32,
}
impl<S> StateWrapper<S> {
    pub(crate) fn new_prepare<C>(&mut self, value: C) -> (Option<MessageKind<C>>, InnerState<C>)
    where
        C: CommandTrait,
    {
        self.ballot = (self.ballot / self.total_nodes + 1) * self.total_nodes + self.proposer_id;
        (
            Some(MessageKind::PrepareMsg(Prepare {
                sender: self.proposer_id,
                ballot: self.ballot,
            })),
            InnerState::Proposal(StateWrapper {
                state: ProposalState {
                    promises: 0,
                    nacks: 0,
                    max_accepted_proposals: HashSet::new(),
                    value,
                },
                ballot: self.ballot,
                proposer_id: self.proposer_id,
                quorum_size: self.quorum_size,
                total_nodes: self.total_nodes,
            }),
        )
    }
}

impl<C> StateWrapper<ProposalState<C>>
where
    C: CommandTrait,
{
    pub fn handle_promise(&mut self, response: Promise<C>) -> (Option<Accept<C>>, InnerState<C>) {
        if let Some(accepted) = response.max_accepted {
            self.state.max_accepted_proposals.insert(accepted);
        }
        self.state.promises += 1;
        if self.state.promises >= self.quorum_size {
            info!(
                "Proposer {}: quorum of {} promises reached (ballot={})",
                self.proposer_id, self.state.promises, self.ballot
            );
            let chosen_value = self
                .state
                .max_accepted_proposals
                .iter()
                .max_by_key(|p| p.ballot)
                .map(|p| p.command.clone())
                .unwrap_or_else(|| self.state.value.clone());

            (
                Some(Accept {
                    sender: self.proposer_id,
                    ballot: self.ballot,
                    command: chosen_value.clone(),
                }),
                InnerState::Accepting(StateWrapper {
                    state: AcceptingState {
                        accepts: 0,
                        nacks: 0,
                        value: chosen_value,
                    },
                    ballot: self.ballot,
                    proposer_id: self.proposer_id,
                    quorum_size: self.quorum_size,
                    total_nodes: self.total_nodes,
                }),
            )
        } else {
            (None, InnerState::Proposal(self.clone()))
        }
    }
}

impl<C> StateWrapper<AcceptingState<C>>
where
    C: CommandTrait,
{
    pub fn handle_ack_accept(
        &mut self,
        ack_accept: AckAccept,
    ) -> (Option<Learn<C>>, InnerState<C>) {
        // old message, maybe due to a crash restart
        if ack_accept.ballot != self.ballot {
            return (None, InnerState::Accepting(self.clone()));
        }
        self.state.accepts += 1;
        if self.state.accepts >= self.quorum_size {
            info!(
                "Proposer {}: quorum of {} accepts reached (ballot={}), sending Learn",
                self.proposer_id, self.state.accepts, self.ballot
            );
            (
                Some(Learn {
                    sender: self.proposer_id,
                    ballot: self.ballot,
                    command: self.state.value.clone(),
                }),
                InnerState::Learning(StateWrapper {
                    state: InitialState {},
                    ballot: self.ballot,
                    proposer_id: self.proposer_id,
                    quorum_size: self.quorum_size,
                    total_nodes: self.total_nodes,
                }),
            )
        } else {
            (None, InnerState::Accepting(self.clone()))
        }
    }
}

#[derive(Debug, Clone)]
enum InnerState<C>
where
    C: CommandTrait,
{
    Initial(StateWrapper<InitialState>),
    Proposal(StateWrapper<ProposalState<C>>),
    Accepting(StateWrapper<AcceptingState<C>>),
    Learning(StateWrapper<InitialState>),
}
fn wrap_message<T: Into<MessageKind<C>> + Debug, C>(
    m: (Option<T>, InnerState<C>),
) -> (Option<MessageKind<C>>, InnerState<C>)
where
    C: CommandTrait,
{
    debug!("Proposer state transition: response={:?}", m.0);
    (m.0.map(Into::into), m.1)
}

impl<C> InnerState<C>
where
    C: CommandTrait,
{
    pub fn new_prepare(&mut self, value: C) -> (Option<MessageKind<C>>, InnerState<C>) {
        match self {
            InnerState::Initial(state_wrapper) => state_wrapper.new_prepare(value),
            InnerState::Proposal(state_wrapper) => state_wrapper.new_prepare(value),
            InnerState::Accepting(state_wrapper) => state_wrapper.new_prepare(value),
            InnerState::Learning(state_wrapper) => state_wrapper.new_prepare(value),
        }
    }
    pub fn handle_message(
        self,
        message: MessageKind<C>,
    ) -> (Option<MessageKind<C>>, InnerState<C>) {
        match (self, message) {
            (InnerState::Initial(mut state), MessageKind::RequestCommandToLeader(val)) => {
                wrap_message(state.new_prepare(val))
            }
            (InnerState::Proposal(mut state_wrapper), MessageKind::PromiseMsg(promise)) => {
                wrap_message(state_wrapper.handle_promise(promise))
            }
            (InnerState::Proposal(mut state_wrapper), MessageKind::NackPrepareMsg(nack)) => {
                state_wrapper.state.nacks += 1;
                if nack.max_ballot > state_wrapper.ballot {
                    state_wrapper.ballot = nack.max_ballot;
                }
                // Can't reach quorum if too many nacks
                if state_wrapper.state.nacks > state_wrapper.total_nodes - state_wrapper.quorum_size
                {
                    let value = state_wrapper.state.value.clone();
                    wrap_message(state_wrapper.new_prepare(value))
                } else {
                    (None, InnerState::Proposal(state_wrapper))
                }
            }
            (InnerState::Accepting(mut state_wrapper), MessageKind::AckAcceptMsg(ack_accept)) => {
                wrap_message(state_wrapper.handle_ack_accept(ack_accept))
            }
            (InnerState::Accepting(mut state_wrapper), MessageKind::NackAcceptMsg(nack)) => {
                state_wrapper.state.nacks += 1;
                if nack.max_ballot > state_wrapper.ballot {
                    state_wrapper.ballot = nack.max_ballot;
                }
                if state_wrapper.state.nacks > state_wrapper.total_nodes - state_wrapper.quorum_size
                {
                    let value = state_wrapper.state.value.clone();
                    wrap_message(state_wrapper.new_prepare(value))
                } else {
                    (None, InnerState::Accepting(state_wrapper))
                }
            }
            r => (None, r.0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Proposer<C>
where
    C: CommandTrait,
{
    inner_state: InnerState<C>,
}

impl<C> Proposer<C>
where
    C: CommandTrait,
{
    pub fn new(proposer_id: u32, quorum_size: u32, total_nodes: u32) -> Self {
        Self {
            inner_state: InnerState::Initial(StateWrapper {
                state: InitialState {},
                ballot: proposer_id,
                proposer_id,
                quorum_size,
                total_nodes,
            }),
        }
    }
    pub fn new_prepare(&mut self, value: C) -> MessageKind<C> {
        let (msg, new_state) = self.inner_state.new_prepare(value);
        self.inner_state = new_state;
        msg.unwrap()
    }
    pub fn handle_message<T: Into<MessageKind<C>> + Debug>(
        &mut self,
        message: T,
    ) -> Option<MessageKind<C>> {
        debug!("Received message: {:?}", message);
        let (resp_message, new_state) = self.inner_state.clone().handle_message(message.into());
        debug!("message: {:?}, {:?}", resp_message, new_state);
        self.inner_state = new_state;
        resp_message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proposer() {
        let mut proposer = Proposer::new(1, 2, 3);
        let _prepare = proposer.new_prepare(123);
        let promise = Promise {
            sender: 2,
            max_accepted: None,
        };
        let resp = proposer.handle_message(promise.clone());
        assert!(resp.is_none());
        let ballot;
        if let InnerState::Proposal(state_wrapper) = &proposer.inner_state {
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
        assert!(matches!(proposer.inner_state, InnerState::Accepting(_)));
        let resp = proposer.handle_message(AckAccept { sender: 2, ballot });
        assert!(resp.is_none());
        assert!(matches!(proposer.inner_state, InnerState::Accepting(_)));
        let resp = proposer.handle_message(AckAccept { sender: 3, ballot });
        assert!(resp.is_some_and(|t| matches!(t, MessageKind::LearnMsg(_))));
        assert!(matches!(proposer.inner_state, InnerState::Learning(_)));
    }

    #[test]
    fn test_proposer_selects_highest_ballot_value() {
        let mut proposer = Proposer::new(1, 2, 3);

        // Proposer starts a new proposal
        let _ = proposer.new_prepare(100);

        // First promise with no accepted value
        let resp = proposer.handle_message(Promise {
            sender: 2,
            max_accepted: Some(MaxAcceptedProposal {
                ballot: 5,
                command: 200,
            }),
        });
        assert!(resp.is_none());

        // Second promise with a higher ballot value
        let resp = proposer.handle_message(Promise {
            sender: 3,
            max_accepted: Some(MaxAcceptedProposal {
                ballot: 10, // Highest ballot seen
                command: 300,
            }),
        });

        // Expect an Accept message with value 300 (from highest ballot)
        assert!(
            matches!(resp, Some(MessageKind::AcceptMsg(Accept { command, .. })) if command == 300)
        );
    }

    #[test]
    fn test_proposer_increments_ballot() {
        let mut proposer = Proposer::new(1, 2, 3);

        let prepare1 = proposer.new_prepare(100);
        if let MessageKind::PrepareMsg(prep) = prepare1 {
            let first_ballot = prep.ballot;

            let prepare2 = proposer.new_prepare(200);
            if let MessageKind::PrepareMsg(prep) = prepare2 {
                let second_ballot = prep.ballot;

                // Ensure ballot is increasing
                assert!(second_ballot > first_ballot);
            } else {
                panic!("Second prepare message is not of type PrepareMsg");
            }
        } else {
            panic!("First prepare message is not of type PrepareMsg");
        }
    }
}
