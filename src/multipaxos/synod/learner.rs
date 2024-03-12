/// Requirements for the paxos learners:
/// the learner should be able to handle different learn messages from different ballots.
/// It should stop caring about messages as soon as it has seen a quorum size of learn messages.
///
use std::collections::HashSet;

use log::debug;

use crate::multipaxos::{Learn, Value};

#[derive(Debug, Clone)]
pub struct Learner {
    value: Option<Value>,
    /// how many voters have voted for this value
    voters: HashSet<u32>,
    ballot: u32,
    pub quorum_size: u32,
}

impl Learner {
    pub(crate) fn new(quorum_size: u32) -> Self {
        Self {
            value: None,
            voters: Default::default(),
            quorum_size,
            ballot: 0,
        }
    }
    pub fn is_value_learned(&self) -> bool {
        self.value.is_some()
    }
    pub fn handle_learn(&mut self, learn_message: Learn) -> anyhow::Result<()> {
        if self.is_value_learned() {
            debug!(
                "Received learn message, but value is already learned. Msg: {:?}",
                learn_message
            );
            // We're done
            return Ok(());
        }
        if learn_message.ballot > self.ballot {
            debug!(
                "Change of ballot, old ballot: {} new ballot: {}, value: {}",
                self.ballot, learn_message.ballot, learn_message.value
            );
            self.ballot = learn_message.ballot;
            self.voters.clear();
        }
        self.voters.insert(learn_message.sender);
        // if we have a quorum, the value is learnt
        if self.voters.len() >= self.quorum_size as usize {
            self.value = Some(learn_message.value);
            debug!(
                "Value was learned! Value: {:?}, voters: {:?}",
                self.value, self.voters
            );
            // clenaup some memory
            self.voters.clear();
        }
        Ok(())
    }
    pub fn value(&self) -> Option<Value> {
        self.value
    }
}

impl Default for Learner {
    fn default() -> Self {
        Self::new(3)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_learner() {
        let mut learner = Learner::new(2);
        learner.handle_learn(Learn {
            value: 1,
            sender: 1,
            ballot: 1,
        });
        assert!(learner.value().is_none());
        learner.handle_learn(Learn {
            value: 1,
            sender: 2,
            ballot: 1,
        });
        assert_eq!(learner.value(), Some(1));

        // at this point, because we reached the quorum, we ignore any other learn message
        learner.handle_learn(Learn {
            value: 2,
            sender: 3,
            ballot: 2,
        });
        learner.handle_learn(Learn {
            value: 2,
            sender: 1,
            ballot: 2,
        });
        assert_eq!(learner.value(), Some(1));
    }

    #[test]
    fn test_learner_change_ballot() {
        let mut learner = Learner::new(2);
        learner.handle_learn(Learn {
            value: 1,
            sender: 1,
            ballot: 1,
        });
        assert!(learner.value().is_none());
        learner.handle_learn(Learn {
            value: 1,
            sender: 2,
            ballot: 2,
        });
        assert!(learner.value().is_none());

        learner.handle_learn(Learn {
            value: 2,
            sender: 3,
            ballot: 3,
        });
        assert!(learner.value().is_none());

        // sender repeated the same message
        learner.handle_learn(Learn {
            value: 2,
            sender: 3,
            ballot: 3,
        });
        assert!(learner.value().is_none());

        learner.handle_learn(Learn {
            value: 2,
            sender: 2,
            ballot: 3,
        });
        assert_eq!(learner.value(), Some(2));
    }
}
