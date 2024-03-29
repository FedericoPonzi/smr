pub use channel::*;
pub use multipaxos::PaxosInstance;

/**
 * A leader process has a unique identifier called the leader identifier. Identifiers are totally ordered.
 * A ballot has a unique identifier as well, called its ballot number. Ballot numbers are totally ordered.
 * ballot numbers be lexicographically ordered pairs of an integer and its leader identifier (consequently, leader identifiers need to be totally ordered as well).
 * This way, given a ballot number, it is trivial to see who the leader of the ballot is.
 */
pub mod channel;
pub mod multipaxos;
mod persistency;

pub trait Command {}

pub trait Result {}

pub trait Applicator {}

pub trait StateMachineReplication<C: Command, R: Result, A: Applicator> {
    fn propose(command: C) -> R;
    fn sync();
    fn register_apply(applicator: A);
}
