use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;

pub use multipaxos::PaxosInstance;

use crate::multipaxos::MessageKind;

/**
 * A leader process has a unique identifier called the leader identifier. Identifiers are totally ordered.
 * A ballot has a unique identifier as well, called its ballot number. Ballot numbers are totally ordered.
 * ballot numbers be lexicographically ordered pairs of an integer and its leader identifier (consequently, leader identifiers need to be totally ordered as well).
 * This way, given a ballot number, it is trivial to see who the leader of the ballot is.
 */
pub mod channel;
pub mod multipaxos;
mod storage;

pub trait StateMachine {
    type Command: Clone;
    type State: Clone;
    fn apply(&mut self, command: Self::Command) -> anyhow::Result<Self::State>;
}

/// Receive a message from the channel for me.
/// Take the message I need to send and deliver it for me.
///
pub trait Channel<C>
where
    C: CommandTrait,
{
    fn receive(&mut self) -> anyhow::Result<Option<MessageKind<C>>>;
    fn send(&mut self, message: MessageKind<C>) -> anyhow::Result<()>;
}

trait StateMachineReplication<SM>
where
    SM: StateMachine,
{
    fn propose(&mut self, command: SM::Command) -> anyhow::Result<SM::State>;
}

#[derive(Debug, Clone)]
pub enum AlgorithmConfig {
    Paxos {},
}
impl Default for AlgorithmConfig {
    fn default() -> Self {
        AlgorithmConfig::Paxos {}
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    // the unique node id.
    node_id: u32,
    // The specific algorithm configuration. Used to select a specific algorithm.
    algorithm: AlgorithmConfig,
    // A list of socket addresses
    cluster: Vec<SocketAddr>,
}

impl Config {
    pub fn node_id(&self) -> u32 {
        self.node_id
    }
}

impl Config {
    pub fn new(
        node_id: u32,
        algorithm: Option<AlgorithmConfig>,
        cluster: Vec<SocketAddr>,
    ) -> anyhow::Result<Config> {
        Ok(Config {
            node_id,
            algorithm: algorithm.unwrap_or_default(),
            cluster,
        })
    }
}

pub trait CommandTrait: Clone + Debug + Ord + Eq + PartialOrd + PartialEq + Hash
// TODO: revisit. Implement them on MaxAcceptedResponse
{
}

impl CommandTrait for u32 {}
