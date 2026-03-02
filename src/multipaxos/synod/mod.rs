pub use acceptor::Acceptor;
pub use learner::Learner;
use log::info;
pub use messages::*;
pub use proposer::Proposer;
use serde::{Deserialize, Serialize};

use crate::CommandTrait;
use crate::multipaxos::Ballot;

mod acceptor;
mod learner;
mod messages;
mod proposer;

#[derive(Debug, Clone, Ord, Eq, PartialOrd, PartialEq, Hash, Deserialize, Serialize)]
pub struct MaxAcceptedProposal<C: CommandTrait> {
    pub ballot: Ballot,
    pub command: C,
}

#[derive(Debug, Clone)]
pub struct PaxosInstance<C>
where
    C: CommandTrait,
{
    acceptor: Acceptor<C>,
    pub(crate) proposer: Proposer<C>,
    learner: Learner<C>,
    instance_id: u64,
}

impl<C> PaxosInstance<C>
where
    C: CommandTrait,
{
    pub fn new(node_id: u32, quorum_size: u32, total_nodes: u32, instance_id: u64) -> Self {
        assert!(
            quorum_size >= 2,
            "quorum_size must be >= 2, got: {}",
            quorum_size
        );
        let acceptor = Acceptor::new(node_id);
        let proposer = Proposer::new(node_id, quorum_size, total_nodes);
        let learner = Learner::new(quorum_size);
        Self {
            acceptor,
            proposer,
            learner,
            instance_id,
        }
    }

    pub fn handle_message<T>(&mut self, message: T) -> anyhow::Result<Option<MessageKind<C>>>
    where
        T: Into<MessageKind<C>>,
        C: CommandTrait,
    {
        let message = message.into();
        info!(
            "PaxosInstance(node={}): handling {:?}",
            self.acceptor.my_id, message
        );
        let result = match message.clone() {
            MessageKind::PrepareMsg(prepare) => {
                let response = self.acceptor.handle_prepare(prepare.clone());
                let node_id = self.acceptor.my_id;
                match &response {
                    MessageKind::PromiseMsg(_) => {
                        let (mvb, mvl) = match &self.acceptor.max_accepted {
                            Some(p) => (Some(p.ballot), Some(format!("{:?}", p.command))),
                            None => (None, None),
                        };
                        super::trace::trace_phase1b(
                            node_id, self.instance_id, prepare.ballot, prepare.sender,
                            self.acceptor.max_ballot, mvb, mvl.as_deref(),
                        );
                    }
                    MessageKind::NackPrepareMsg(_) => {
                        super::trace::trace_nack_prepare(
                            node_id, self.instance_id, prepare.ballot, prepare.sender,
                            self.acceptor.max_ballot,
                        );
                    }
                    _ => {}
                }
                Some(response)
            }
            MessageKind::PromiseMsg(promise) => {
                let response = self
                    .proposer
                    .handle_message(MessageKind::PromiseMsg(promise));
                if let Some(MessageKind::AcceptMsg(ref accept)) = response {
                    super::trace::trace_phase2a(
                        self.acceptor.my_id, self.instance_id, accept.ballot,
                        &format!("{:?}", accept.command),
                    );
                }
                response
            }
            MessageKind::AcceptMsg(accept) => {
                let response = self.acceptor.handle_accept(accept.clone());
                let node_id = self.acceptor.my_id;
                match &response {
                    MessageKind::AckAcceptMsg(_) => {
                        let max_acc = self.acceptor.max_accepted.as_ref().unwrap();
                        super::trace::trace_phase2b(
                            node_id, self.instance_id, accept.ballot, accept.sender,
                            self.acceptor.max_ballot, max_acc.ballot,
                            &format!("{:?}", max_acc.command),
                        );
                    }
                    MessageKind::NackAcceptMsg(_) => {
                        super::trace::trace_nack_accept(
                            node_id, self.instance_id, accept.ballot, accept.sender,
                            self.acceptor.max_ballot,
                        );
                    }
                    _ => {}
                }
                Some(response)
            }
            MessageKind::LearnMsg(learn) => {
                let already_learned = self.learner.is_value_learned();
                self.learner.handle_learn(learn.clone())?;
                if !already_learned && self.learner.is_value_learned() {
                    super::trace::trace_learn(
                        self.acceptor.my_id, self.instance_id, learn.ballot,
                        &format!("{:?}", learn.command),
                    );
                }
                // Re-broadcast with our own ID to help other learners reach quorum,
                // but only if: we haven't already learned AND sender is not us
                if !already_learned && learn.sender != self.acceptor.my_id {
                    Some(MessageKind::LearnMsg(Learn {
                        sender: self.acceptor.my_id,
                        ballot: learn.ballot,
                        command: learn.command,
                    }))
                } else {
                    None
                }
            }
            MessageKind::AckAcceptMsg(msg) => {
                self.proposer.handle_message(MessageKind::AckAcceptMsg(msg))
            }
            MessageKind::NackPrepareMsg(nack) => self
                .proposer
                .handle_message(MessageKind::NackPrepareMsg(nack)),
            MessageKind::NackAcceptMsg(nack) => self
                .proposer
                .handle_message(MessageKind::NackAcceptMsg(nack)),
            MessageKind::RequestCommandToLeader(_cmd) => {
                let response = self.proposer.handle_message(message);
                if let Some(MessageKind::PrepareMsg(ref prep)) = response {
                    super::trace::trace_phase1a(
                        self.acceptor.my_id, self.instance_id, prep.ballot,
                    );
                }
                response
            }
            _ => None,
        };
        Ok(result)
    }
    pub fn get_value(&self) -> Option<C> {
        self.learner.value()
    }
}

#[cfg(test)]
mod tests {
    use crate::PaxosInstance;
    use crate::multipaxos::MessageKind::RequestCommandToLeader;
    use crate::multipaxos::{Accept, AckAccept, MessageKind, Prepare, Promise};

    #[test]
    pub fn test_paxosinstance_simple() -> anyhow::Result<()> {
        let mut paxos: PaxosInstance<u32> = PaxosInstance::new(1, 2, 3, 0);
        let to = 5;
        for i in 1..=to {
            let promise: MessageKind<u32> = paxos
                .handle_message(Prepare {
                    sender: 100,
                    ballot: i,
                })?
                .unwrap();
            assert!(
                matches!(promise, MessageKind::PromiseMsg(_),),
                "{:?}",
                promise
            );
        }
        let propose = paxos.handle_message(RequestCommandToLeader(123))?;
        let proposer_ballot = match &propose {
            Some(MessageKind::PrepareMsg(p)) => p.ballot,
            _ => panic!("expected PrepareMsg, got {:?}", propose),
        };

        // lower ballot, acceptor returns nack
        let promise: Option<MessageKind<u32>> = paxos.handle_message(Prepare {
            sender: 100,
            ballot: 1,
        })?;
        assert!(
            matches!(promise, Some(MessageKind::NackPrepareMsg(_))),
            "{:?}",
            paxos
        );
        // equal ballot, acceptor returns nack
        let promise: Option<MessageKind<u32>> = paxos.handle_message(Prepare {
            sender: 100,
            ballot: to,
        })?;
        assert!(
            matches!(promise, Some(MessageKind::NackPrepareMsg(_))),
            "{:?}",
            paxos
        );

        let resp: Option<MessageKind<u32>> = paxos.handle_message(Promise {
            sender: 101,
            ballot: proposer_ballot,
            max_accepted: None,
        })?;
        assert_eq!(resp, None, "{:?}", paxos);

        let resp: Option<MessageKind<u32>> = paxos.handle_message(Promise {
            sender: 102,
            ballot: proposer_ballot,
            max_accepted: None,
        })?;
        assert!(
            matches!(resp, Some(MessageKind::AcceptMsg(_))),
            "{:?}, paxos: {:?}",
            resp,
            paxos
        );

        // acceptors should return ack message
        let resp = paxos
            .handle_message(Accept {
                sender: 103,
                ballot: to,
                command: 123,
            })?
            .unwrap();
        assert!(matches!(resp, MessageKind::AckAcceptMsg(_),), "{:?}", resp);

        // when proposer sees a quorum of ack accept, it should issue a new learn message
        let resp: Option<MessageKind<u32>> = paxos.handle_message(AckAccept {
            sender: 104,
            ballot: to,
        })?;
        assert_eq!(resp, None, "{:?}", paxos);
        let resp: Option<MessageKind<u32>> = paxos.handle_message(AckAccept {
            sender: 105,
            ballot: to,
        })?;
        assert_eq!(resp, None, "{:?}", paxos);

        Ok(())
    }
}
