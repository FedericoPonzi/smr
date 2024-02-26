use std::collections::HashMap;
use crate::multipaxos::{Accepted, Value};

pub struct Learner {
    value: Option<Value>,
    /// how many voters have voted for this value
    voters: HashMap<Value, u32>,
    pub number_of_acceptors: u32,
}
pub struct LearnerState {}
impl Learner {
    fn new(number_of_acceptors: u32) -> Self {
        Self {
            value: None,
            voters: HashMap::new(),
            number_of_acceptors,
        }
    }
    fn handle_learn(&mut self, learn_message: Accepted) {
        if self.value.is_some() {
            return;
        }
        let entry = self.voters.entry(learn_message.value).or_insert(0);
        *entry += 1;
        // if we have a quorum, the value is learnt
        if *entry * 2 > self.number_of_acceptors {
            self.value = Some(learn_message.value);
        }
    }
}
