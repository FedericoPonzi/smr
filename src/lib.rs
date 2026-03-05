//! State Machine Replication (SMR) implementation using Multi-Paxos consensus protocol
//!
//! This module provides the core abstractions and implementations for building replicated state machines:
//! - StateMachine trait for defining replicated state machines
//! - Multi-Paxos based consensus for ensuring consistency across replicas
//! - Network transport layer for communication between nodes
//! - Runtime for managing the replication protocol

use crate::multipaxos::storage::CommandLog;
use crate::multipaxos::{Message, MultiPaxosNode};
pub use multipaxos::PaxosInstance;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, info};

pub mod channel;
pub mod multipaxos;
pub mod storage;

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

pub type ProposalResult<S> = (
    Vec<Message<<S as StateMachine>::Command>>,
    oneshot::Receiver<<S as StateMachine>::Output>,
);
pub type CommitResult<S> = (
    <S as StateMachine>::Command,
    Option<oneshot::Sender<<S as StateMachine>::Output>>,
);

pub trait StateMachineReplicationAlgorithm<S>
where
    S: StateMachine,
{
    type SMRMessage;
    fn propose(&mut self, command: S::Command) -> Result<ProposalResult<S>>;
    fn handle_message(&mut self, message: Self::SMRMessage) -> Result<Vec<Message<S::Command>>>;
    fn get_commit_id(&mut self, id: u64) -> Option<CommitResult<S>>;
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
    pub other_nodes: Vec<(u32, String)>,
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
        other_nodes: Vec<(u32, String)>,
    ) -> Result<SmrConfig> {
        Ok(SmrConfig {
            node_id,
            bind_address: bind_address.unwrap_or("127.0.0.1".to_owned()),
            total_nodes: other_nodes.len() as u32 + 1,
            other_nodes,
        })
    }

    /// Parse config from CLI args: `<binary> <node_id> <port1,port2,...,portN>`
    pub fn from_cli_args() -> Result<SmrConfig> {
        use std::env;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let args: Vec<String> = env::args().collect();

        if args.len() != 3 {
            anyhow::bail!(
                "Usage: {} <node_id> <port1,port2,...,portN>  (N >= 3)",
                args[0]
            );
        }

        let node_id: u16 = args[1]
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid node ID"))?;

        let ports: Vec<u16> = args[2]
            .split(',')
            .map(|s| {
                s.parse()
                    .map_err(|_| anyhow::anyhow!("Invalid port number"))
            })
            .collect::<std::result::Result<_, _>>()?;

        let num_nodes = ports.len();
        if num_nodes < 3 {
            anyhow::bail!(
                "Must provide at least 3 port numbers separated by commas, got {}",
                num_nodes
            );
        }
        if node_id as usize >= num_nodes {
            anyhow::bail!(
                "Node ID must be in range 0..{} (got {})",
                num_nodes - 1,
                node_id
            );
        }

        let bind_address =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ports[node_id as usize]);

        let other_nodes = (0..num_nodes)
            .filter(|&n| n != node_id as usize)
            .map(|n| {
                (
                    n as u32,
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ports[n]).to_string(),
                )
            })
            .collect();

        SmrConfig::new(node_id as u32, Some(bind_address.to_string()), other_nodes)
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
    pending_proposals: HashMap<u64, CommitResult<S>>,
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

    /// Create a runtime with persistent storage. Replays command log to restore state machine.
    pub fn with_storage(
        config: SmrConfig,
        mut state_machine: S,
        paxos_storage: multipaxos::storage::PaxosStorage<S::Command>,
        command_log: CommandLog<S::Command>,
    ) -> Result<Self> {
        // Replay command log to restore state machine
        let entries = command_log.replay()?;
        let last_applied = command_log.last_applied()?;
        for (_, cmd) in entries {
            state_machine.apply(cmd)?;
        }
        info!(
            "Recovered state machine: replayed up to instance {}",
            last_applied
        );

        let inner = Arc::new(Mutex::new(SmrRuntimeInner::with_storage(
            config,
            state_machine,
            paxos_storage,
            command_log,
            last_applied,
        )?));
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

#[allow(dead_code)]
struct SmrRuntimeInner<S: StateMachine> {
    config: SmrConfig,
    algorithm: Arc<Mutex<MultiPaxosNode<S>>>,
    state_machine: Arc<RwLock<S>>,
    last_applied_command_id: Arc<RwLock<u64>>,
    command_log: Option<Arc<Mutex<CommandLog<S::Command>>>>,
    outbox: mpsc::Sender<Message<S::Command>>,
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
                .map(|(id, addr)| (*id, addr.parse().unwrap()))
                .collect(),
        );
        let state_machine = Arc::new(RwLock::new(state_machine));
        let (outbox_tx, outbox_rx) = mpsc::channel(100);
        let algorithm_cl = Arc::clone(&algorithm);
        let last_applied_command_id = Arc::new(RwLock::new(0u64));
        let last_applied_command_id_cl = last_applied_command_id.clone();
        let state_machine_cl = state_machine.clone();

        let handle = tokio::spawn(async {
            Self::background(
                channel,
                algorithm_cl,
                last_applied_command_id_cl,
                state_machine_cl,
                outbox_rx,
                None,
            )
            .await
        });

        Ok(Self {
            config,
            algorithm,
            state_machine,
            last_applied_command_id,
            command_log: None,
            outbox: outbox_tx,
            ch_handle: handle,
        })
    }

    pub fn with_storage(
        config: SmrConfig,
        state_machine: S,
        paxos_storage: multipaxos::storage::PaxosStorage<S::Command>,
        command_log: CommandLog<S::Command>,
        last_applied: u64,
    ) -> Result<Self>
    where
        <S as StateMachine>::Command: 'static,
    {
        debug!("Initializing SmrRuntimeInner with storage recovery");
        let algorithm = Arc::new(Mutex::new(MultiPaxosNode::with_storage(
            config.clone(),
            paxos_storage,
        )?));
        let channel: TcpChannel<Message<S::Command>> = TcpChannel::new(
            config.node_id,
            config.bind_address.parse()?,
            config
                .other_nodes
                .iter()
                .map(|(id, addr)| (*id, addr.parse().unwrap()))
                .collect(),
        );
        let state_machine = Arc::new(RwLock::new(state_machine));
        let (outbox_tx, outbox_rx) = mpsc::channel(100);
        let algorithm_cl = Arc::clone(&algorithm);
        let last_applied_command_id = Arc::new(RwLock::new(last_applied));
        let last_applied_command_id_cl = last_applied_command_id.clone();
        let state_machine_cl = state_machine.clone();
        let command_log = Arc::new(Mutex::new(command_log));
        let command_log_cl = Arc::clone(&command_log);

        let handle = tokio::spawn(async {
            Self::background(
                channel,
                algorithm_cl,
                last_applied_command_id_cl,
                state_machine_cl,
                outbox_rx,
                Some(command_log_cl),
            )
            .await
        });

        Ok(Self {
            config,
            algorithm,
            state_machine,
            last_applied_command_id,
            command_log: Some(command_log),
            outbox: outbox_tx,
            ch_handle: handle,
        })
    }
    async fn background(
        mut channel: TcpChannel<Message<S::Command>>,
        algorithm: Arc<Mutex<MultiPaxosNode<S>>>,
        last_applied_command_id: Arc<RwLock<u64>>,
        state_machine: Arc<RwLock<S>>,
        mut outbox: mpsc::Receiver<Message<S::Command>>,
        command_log: Option<Arc<Mutex<CommandLog<S::Command>>>>,
    ) {
        channel.start().await;
        info!("Background loop started, listening for messages");
        loop {
            tokio::select! {
                // Messages from the network → process through algorithm
                result = channel.receive() => {
                    let Some(msg) = result else { break };
                    info!("Network: received message for instance {}", msg.instance_id());
                    let mut algorithm_lc = algorithm.lock().await;
                    let responses = algorithm_lc.handle_message(msg).unwrap();
                    for response in responses {
                        info!("Network: sending response for instance {}", response.instance_id());
                        channel.send(response).await;
                    }
                    let mut last_applied_commit = last_applied_command_id.write().await;
                    while let Some((command, sender)) = algorithm_lc.get_commit_id(*last_applied_commit) {
                        if let Some(ref cl) = command_log {
                            let mut log = cl.lock().await;
                            let _ = log.append(*last_applied_commit, &command);
                        }
                        let mut sm = state_machine.write().await;
                        if let Ok(output) = sm.apply(command) {
                            info!("Applied command for instance {}", *last_applied_commit);
                            if let Some(ref cl) = command_log {
                                let mut log = cl.lock().await;
                                let _ = log.set_last_applied(*last_applied_commit + 1);
                            }
                            if let Some(sender) = sender {
                                let _ = sender.send(output);
                            }
                        }
                        *last_applied_commit += 1;
                    }
                }
                // Messages from propose() → already processed, just forward to TCP
                result = outbox.recv() => {
                    let Some(msg) = result else { break };
                    channel.send(msg).await;
                }
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
            self.outbox.send(m).await?;
        }
        Ok(resp)
    }
}
