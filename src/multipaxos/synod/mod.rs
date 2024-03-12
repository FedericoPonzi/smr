pub use acceptor::Acceptor;
pub use learner::Learner;
pub use messages::*;
pub use proposer::Proposer;

use crate::multipaxos::{Ballot, Value};

mod acceptor;
mod learner;
mod messages;
mod proposer;

#[derive(Debug, Clone, Ord, Eq, PartialOrd, PartialEq, Hash)]
pub struct MaxAcceptedProposal {
    pub ballot: Ballot,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct PaxosInstance {
    acceptor: Acceptor,
    proposer: Proposer,
    learner: Learner,
    quorum_size: u32,
}

impl PaxosInstance {
    pub fn new(quorum_size: u32) -> Self {
        assert!(quorum_size >= 2);
        let my_id = 0;
        let acceptor = Acceptor::new(my_id);
        let proposer = Proposer::new(my_id, quorum_size, 321);
        let learner = Learner::new(quorum_size);
        Self {
            acceptor,
            proposer,
            learner,
            quorum_size,
        }
    }
    pub fn prepare_phase1(&mut self) -> anyhow::Result<Message> {
        let prepare_msg = self.proposer.new_prepare();
        Ok(prepare_msg.into())
    }

    pub fn handle_message<T: Into<Message>>(
        &mut self,
        message: T,
    ) -> anyhow::Result<Option<Message>> {
        let message = message.into();
        match message {
            Message::PrepareMsg(prepare) => {
                Ok(self.acceptor.handle_prepare(prepare).map(Into::into))
            }
            Message::PromiseMsg(promise) => {
                let resp = self.proposer.handle_promise(promise);
                Ok(resp.map(Into::into))
            }
            Message::AcceptMsg(accept) => {
                let resp = self.acceptor.handle_accept(accept);
                Ok(resp.map(Into::into))
            }
            Message::LearnMsg(learn) => {
                self.learner.handle_learn(learn)?;
                Ok(None)
            }
            Message::AckAcceptMsg(msg) => Ok(self.proposer.handle_ack_accept(msg).map(Into::into)),
        }
    }
    pub fn get_value(&self) -> Option<Value> {
        self.learner.value()
    }
}

#[cfg(test)]
mod tests {
    use crate::multipaxos::{Accept, AckAccept, Message, Prepare, Promise};
    use crate::PaxosInstance;

    #[test]
    pub fn test_paxosinstance_simple() -> anyhow::Result<()> {
        let mut paxos = PaxosInstance::new(2);
        let to = 5;
        for i in 1..=to {
            let promise = paxos
                .handle_message(Prepare {
                    sender: 100,
                    ballot: i,
                })?
                .unwrap();
            assert!(matches!(promise, Message::PromiseMsg(_),), "{:?}", promise);
        }

        // lower ballot, acceptor doesn't care
        let promise = paxos.handle_message(Prepare {
            sender: 100,
            ballot: 1,
        })?;
        assert_eq!(promise, None, "{:?}", paxos);
        // equal ballot, acceptor doesn't care
        let promise = paxos.handle_message(Prepare {
            sender: 100,
            ballot: to,
        })?;
        assert_eq!(promise, None, "{:?}", paxos);

        let resp = paxos.handle_message(Promise {
            sender: 101,
            max_accepted: None,
        })?;
        assert_eq!(resp, None, "{:?}", paxos);

        let resp = paxos
            .handle_message(Promise {
                sender: 102,
                max_accepted: None,
            })?
            .unwrap();
        assert!(matches!(resp, Message::AcceptMsg(_),), "{:?}", resp);

        // acceptors should return ack message
        let resp = paxos
            .handle_message(Accept {
                sender: 103,
                ballot: to,
                value: 123,
            })?
            .unwrap();
        assert!(matches!(resp, Message::AckAcceptMsg(_),), "{:?}", resp);

        // when proposer sees a quorum of ack accept, it should issue a new learn message
        let resp = paxos.handle_message(AckAccept {
            sender: 104,
            ballot: to,
        })?;
        assert_eq!(resp, None, "{:?}", paxos);
        let resp = paxos.handle_message(AckAccept {
            sender: 105,
            ballot: to,
        })?;
        assert_eq!(resp, None, "{:?}", paxos);

        Ok(())
    }
}
