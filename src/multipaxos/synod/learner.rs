/// Requirements for the paxos learners:
/// the learner should be able to handle different learn messages from different ballots.
/// It should stop caring about messages as soon as it has seen a quorum size of learn messages.
///
use std::collections::{HashMap, HashSet};

use log::{debug, info};

use crate::CommandTrait;
use crate::multipaxos::{Learn, MessageKind};

#[derive(Debug, Clone)]
pub struct Learner<C>
where
    C: CommandTrait,
{
    value: Option<C>,
    /// how many voters have voted for this value
    voters: HashSet<u32>,
    votes: HashMap<C, u32>, // Tracks which command has how many votes
    ballot: u32,
    pub quorum_size: u32,
}

impl<C> Learner<C>
where
    C: CommandTrait,
{
    pub(crate) fn new(quorum_size: u32) -> Self {
        Self {
            value: None,
            voters: Default::default(),
            votes: Default::default(),
            quorum_size,
            ballot: 0,
        }
    }
    pub fn is_value_learned(&self) -> bool {
        self.value.is_some()
    }
    pub fn handle_learn(
        &mut self,
        learn_message: Learn<C>,
    ) -> anyhow::Result<Option<MessageKind<C>>> {
        if self.is_value_learned() {
            debug!(
                "Received learn message, but value is already learned. Msg: {:?}",
                learn_message
            );
            // We're done
            return Ok(None);
        }
        if learn_message.ballot > self.ballot {
            debug!(
                "Change of ballot, old ballot: {} new ballot: {}",
                self.ballot,
                learn_message.ballot //, learn_message.command
            );
            self.ballot = learn_message.ballot;
            self.voters.clear();
            self.votes.clear();
            self.value = None; // Reset learned value for the new ballot
        }

        if learn_message.ballot == self.ballot && self.voters.contains(&learn_message.sender) {
            debug!(
                "Ignoring duplicate learn message from sender {:?}",
                learn_message.sender
            );
            return Ok(None);
        }

        self.voters.insert(learn_message.sender);

        let votes = self
            .votes
            .entry(learn_message.command.clone())
            .or_insert_with(|| 0);
        *votes += 1;
        debug!(
            "Learn message received: {:?}, votes: {}",
            learn_message, votes
        );

        // if we have a quorum, the value is learnt
        if *votes >= self.quorum_size {
            self.value = Some(learn_message.command.clone());
            info!(
                "Learner: value learned with {} votes! command={:?}",
                votes, learn_message.command
            );
            // clenaup some memory
            self.voters.clear();
            self.votes.clear();
            return Ok(Some(MessageKind::LearnedCommand {
                cmd: learn_message.command,
            }));
        }
        Ok(None)
    }
    pub fn value(&self) -> Option<C> {
        self.value.clone()
    }
}

impl<C> Default for Learner<C>
where
    C: CommandTrait,
{
    fn default() -> Self {
        Self::new(3)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_learner() -> anyhow::Result<()> {
        let mut learner = Learner::new(2);
        learner.handle_learn(Learn {
            command: 1,
            sender: 1,
            ballot: 1,
        })?;
        assert!(learner.value().is_none());
        learner.handle_learn(Learn {
            command: 1,
            sender: 2,
            ballot: 1,
        })?;
        assert_eq!(learner.value(), Some(1));

        // at this point, because we reached the quorum, we ignore any other learn message
        learner.handle_learn(Learn {
            command: 2,
            sender: 3,
            ballot: 2,
        })?;
        learner.handle_learn(Learn {
            command: 2,
            sender: 1,
            ballot: 2,
        })?;
        assert_eq!(learner.value(), Some(1));
        Ok(())
    }

    #[test]
    fn test_learner_change_ballot() -> anyhow::Result<()> {
        let mut learner = Learner::new(2);
        learner.handle_learn(Learn {
            command: 1,
            sender: 1,
            ballot: 1,
        })?;
        assert!(learner.value().is_none());
        learner.handle_learn(Learn {
            command: 1,
            sender: 2,
            ballot: 2,
        })?;
        assert!(learner.value().is_none());

        learner.handle_learn(Learn {
            command: 2,
            sender: 3,
            ballot: 3,
        })?;
        assert!(learner.value().is_none());

        // sender repeated the same message
        learner.handle_learn(Learn {
            command: 2,
            sender: 3,
            ballot: 3,
        })?;
        assert!(learner.value().is_none());

        learner.handle_learn(Learn {
            command: 2,
            sender: 2,
            ballot: 3,
        })?;
        assert_eq!(learner.value(), Some(2));
        Ok(())
    }

    #[test]
    fn test_learner_conflicting_values_same_ballot() -> anyhow::Result<()> {
        let mut learner = Learner::new(2);
        learner.handle_learn(Learn {
            command: 1,
            sender: 1,
            ballot: 1,
        })?;
        learner.handle_learn(Learn {
            command: 2, // Conflicting value!
            sender: 2,
            ballot: 1,
        })?;
        assert!(
            learner.value().is_none(),
            "Value should not be learned yet: {:?}",
            learner.value()
        ); // No value should be learned yet

        learner.handle_learn(Learn {
            command: 1,
            sender: 3,
            ballot: 1,
        })?;
        assert_eq!(learner.value(), Some(1)); // 1 wins with quorum
        Ok(())
    }
}
