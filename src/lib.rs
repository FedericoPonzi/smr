//! State Machine Replication (SMR) implementation using Multi-Paxos consensus protocol
//!
//! This module provides the core abstractions and implementations for building replicated state machines:
//! - StateMachine trait for defining replicated state machines
//! - Multi-Paxos based consensus for ensuring consistency across replicas
//! - Network transport layer for communication between nodes
//! - Runtime for managing the replication protocol

use crate::multipaxos::{Message, MultiPaxosNode};
pub use multipaxos::PaxosInstance;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::debug;

pub mod channel;
pub mod multipaxos;
mod storage;

pub use channel::TcpChannel;

pub type Result<T> = anyhow::Result<T>;

pub trait StateMachine: Sync + Send {
    type Command: SerializableCommand;
    type Output: Clone + Send;

    fn apply(&mut self, command: Self::Command) -> Result<Self::Output>;

    fn init(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Receive a message from the channel for me.
/// Take the message I need to send and deliver it for me.
///
pub trait Channel<C>
where
    C: Serialize + for<'a> Deserialize<'a>,
{
    fn receive(&mut self) -> Result<Option<C>>;
    fn send(&mut self, message: C) -> Result<()>;
}

pub trait StateMachineReplicationAlgorithm<S>
where
    S: StateMachine,
{
    type SMRMessage;
    fn propose(
        &mut self,
        command: S::Command,
    ) -> Result<(Vec<Message<S::Command>>, oneshot::Receiver<S::Output>)>;
    fn handle_message(&mut self, message: Self::SMRMessage) -> Result<Vec<Message<S::Command>>>;
    fn get_commit_id(&mut self, id: u64) -> Option<S::Command>;
}

#[derive(Debug, Clone)]
pub enum AlgorithmConfig {
    Paxos,
}
impl Default for AlgorithmConfig {
    fn default() -> Self {
        AlgorithmConfig::Paxos {}
    }
}

#[derive(Debug, Clone)]
pub struct SmrConfig {
    // the unique node id.
    pub node_id: u32,
    // A list of socket addresses
    total_nodes: u32,
    pub other_nodes: Vec<String>,
    pub bind_address: String,
}

impl SmrConfig {
    pub fn node_id(&self) -> u32 {
        self.node_id
    }
}

impl SmrConfig {
    pub fn new(
        node_id: u32,
        bind_address: Option<String>,
        other_nodes: Vec<String>,
    ) -> Result<SmrConfig> {
        Ok(SmrConfig {
            node_id,
            bind_address: bind_address.unwrap_or("127.0.0.1".to_owned()),
            total_nodes: other_nodes.len() as u32,
            other_nodes,
        })
    }
}

// TODO: revisit. Implement them on MaxAcceptedResponse
pub trait CommandTrait: Clone + Debug + Eq + PartialEq + Hash + Send + Sync {}

impl<T> CommandTrait for T where T: Clone + Debug + Eq + PartialEq + Hash + Send + Sync {}

// Define a helper trait
pub trait SerializableCommand: CommandTrait + Serialize + Debug + for<'a> Deserialize<'a> {}
// Implement it automatically for all types satisfying the bounds
impl<T> SerializableCommand for T where T: CommandTrait + Serialize + for<'a> Deserialize<'a> {}

pub struct SmrRuntime<S: StateMachine + 'static> {
    pending_proposals: HashMap<u64, (S::Command, Option<oneshot::Sender<S::Output>>)>,
    // this is just a counter.
    next_proposal_id: u64,
    inner: Arc<Mutex<SmrRuntimeInner<S>>>,
}

impl<S> SmrRuntime<S>
where
    S: StateMachine + Send,
{
    pub fn new(config: SmrConfig, state_machine: S) -> Result<Self> {
        let inner = Arc::new(Mutex::new(SmrRuntimeInner::new(config, state_machine)?));
        Ok(Self {
            inner,
            pending_proposals: HashMap::new(),
            next_proposal_id: 0,
        })
    }
    pub async fn propose(&mut self, v: S::Command) -> Result<oneshot::Receiver<S::Output>> {
        let mut inner = self.inner.lock().await;
        let id = self.next_proposal_id;
        self.next_proposal_id += 1;
        self.pending_proposals.insert(id, (v.clone(), None));
        inner.propose(v).await
    }
}

struct SmrRuntimeInner<S: StateMachine> {
    config: SmrConfig,
    algorithm: Arc<Mutex<MultiPaxosNode<S>>>,
    state_machine: Arc<RwLock<S>>,
    last_applied_command_id: Arc<RwLock<u64>>,
    channel: mpsc::Sender<Message<S::Command>>,
    ch_handle: JoinHandle<()>,
}
impl<S> SmrRuntimeInner<S>
where
    S: StateMachine + 'static,
{
    pub fn new(config: SmrConfig, state_machine: S) -> Result<Self>
    where
        <S as StateMachine>::Command: 'static,
    {
        debug!("Initializing SmrRuntimeInner");
        let algorithm = Arc::new(Mutex::new(MultiPaxosNode::new(config.clone())));
        let channel: TcpChannel<Message<S::Command>> = TcpChannel::new(
            config.node_id,
            config.bind_address.parse()?,
            config
                .other_nodes
                .iter()
                .map(|n| {
                    (
                        n.split(":").collect::<Vec<&str>>()[1].parse().unwrap(),
                        n.clone().parse().unwrap(),
                    )
                })
                .collect(),
        );
        let state_machine = Arc::new(RwLock::new(state_machine));
        let sender = channel.sender.clone();
        let algorithm_cl = Arc::clone(&algorithm); // Clone Arc for async task
        let last_applied_command_id = Arc::new(RwLock::new(0u64));
        let last_applied_command_id_cl = last_applied_command_id.clone();
        let state_machine_cl = state_machine.clone();

        let handle = tokio::spawn(async {
            Self::background(
                channel,
                algorithm_cl,
                last_applied_command_id_cl,
                state_machine_cl,
            )
            .await
        });

        Ok(Self {
            config,
            algorithm,
            state_machine,
            last_applied_command_id,
            channel: sender,
            ch_handle: handle,
        })
    }
    async fn background(
        mut channel: TcpChannel<Message<S::Command>>,
        algorithm: Arc<Mutex<MultiPaxosNode<S>>>,
        last_applied_command_id: Arc<RwLock<u64>>,
        state_machine: Arc<RwLock<S>>,
    ) {
        while let Some(msg) = channel.receive().await {
            let mut algorithm_lc = algorithm.lock().await;
            let responses = algorithm_lc.handle_message(msg).unwrap();
            for response in responses {
                channel.send(response).await;
            }
            let last_applied_commit = last_applied_command_id.write().await;

            if let Some(command) = algorithm_lc.get_commit_id(*last_applied_commit) {
                let mut sm = state_machine.write().await;
                let res = sm.apply(command);
                //todo: something wrong, would need to forward result to the listening client
            }
        }
    }
    pub async fn propose(&mut self, cmd: S::Command) -> Result<oneshot::Receiver<S::Output>>
    where
        <S as StateMachine>::Command: 'static,
    {
        let mut alg = self.algorithm.lock().await;
        // TODO: this might not be enough, as a proposal might not go through in the current round.
        // but it's a start. ideally, this adds to a proposal list, and another thread
        // continuosly tries to push proposals
        let (ret, resp) = alg.propose(cmd)?;
        for m in ret {
            self.channel.send(m).await?;
        }
        Ok(resp)
    }
}
