pub use acceptor::Acceptor;
pub use learner::Learner;
pub use messages::*;
pub use proposer::Proposer;

use crate::multipaxos::Ballot;
use crate::CommandTrait;

mod acceptor;
mod learner;
mod messages;
mod proposer;

#[derive(Debug, Clone, Ord, Eq, PartialOrd, PartialEq, Hash)]
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
    proposer: Proposer<C>,
    learner: Learner<C>,
    quorum_size: u32,
}

impl<C> PaxosInstance<C>
where
    C: CommandTrait,
{
    pub fn new(quorum_size: u32) -> Self {
        assert!(quorum_size >= 2);
        let my_id = 1;
        let acceptor = Acceptor::new(my_id);
        let proposer = Proposer::new(my_id, quorum_size);
        let learner = Learner::new(quorum_size);
        Self {
            acceptor,
            proposer,
            learner,
            quorum_size,
        }
    }

    pub fn handle_message<T>(&mut self, message: T) -> anyhow::Result<Option<MessageKind<C>>>
    where
        T: Into<MessageKind<C>>,
        C: CommandTrait,
    {
        let message = message.into();
        match message.clone() {
            MessageKind::PrepareMsg(prepare) => {
                Ok(self.acceptor.handle_prepare(prepare).map(Into::into))
            }
            MessageKind::PromiseMsg(promise) => {
                let resp = self
                    .proposer
                    .handle_message(MessageKind::PromiseMsg(promise));
                Ok(resp)
            }
            MessageKind::AcceptMsg(accept) => {
                let resp = self.acceptor.handle_accept(accept);
                Ok(resp.map(Into::into))
            }
            MessageKind::LearnMsg(learn) => {
                self.learner.handle_learn(learn)?;
                Ok(None)
            }
            MessageKind::AckAcceptMsg(msg) => {
                Ok(self.proposer.handle_message(MessageKind::AckAcceptMsg(msg)))
            }
            MessageKind::RequestCommandToLeader(_cmd) => {
                let resp = self.proposer.handle_message(message);
                Ok(resp)
            }
        }
    }
    pub fn get_value(&self) -> Option<C> {
        self.learner.value()
    }
}

#[cfg(test)]
mod tests {
    use crate::multipaxos::MessageKind::RequestCommandToLeader;
    use crate::multipaxos::{Accept, AckAccept, MessageKind, Prepare, Promise};
    use crate::PaxosInstance;

    #[test]
    pub fn test_paxosinstance_simple() -> anyhow::Result<()> {
        let mut paxos: PaxosInstance<u32> = PaxosInstance::new(2);
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
        assert!(
            matches!(propose, Some(MessageKind::PrepareMsg(_)),),
            "{:?}",
            propose
        );

        // lower ballot, acceptor doesn't care
        let promise: Option<MessageKind<u32>> = paxos.handle_message(Prepare {
            sender: 100,
            ballot: 1,
        })?;
        assert_eq!(promise, None, "{:?}", paxos);
        // equal ballot, acceptor doesn't care
        let promise: Option<MessageKind<u32>> = paxos.handle_message(Prepare {
            sender: 100,
            ballot: to,
        })?;
        assert_eq!(promise, None, "{:?}", paxos);

        let resp: Option<MessageKind<u32>> = paxos.handle_message(Promise {
            sender: 101,
            max_accepted: None,
        })?;
        assert_eq!(resp, None, "{:?}", paxos);

        let resp: Option<MessageKind<u32>> = paxos.handle_message(Promise {
            sender: 102,
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
