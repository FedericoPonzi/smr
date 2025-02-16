//! State Machine Replication (SMR) implementation using Multi-Paxos consensus protocol
//!
//! This module provides the core abstractions and implementations for building replicated state machines:
//! - StateMachine trait for defining replicated state machines
//! - Multi-Paxos based consensus for ensuring consistency across replicas
//! - Network transport layer for communication between nodes
//! - Runtime for managing the replication protocol

use crate::multipaxos::{Message, MessageKind, MultiPaxosNode};
use log::info;
pub use multipaxos::PaxosInstance;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

pub mod channel;
pub mod multipaxos;
mod storage;

pub use channel::TcpChannel;

pub type Result<T> = anyhow::Result<T>;

pub trait StateMachine {
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

pub trait StateMachineReplicationAlgorithm<T>
where
    T: SerializableCommand,
{
    type SMRMessage;
    fn propose(&mut self, command: T) -> Result<Vec<Message<T>>>;
    fn handle_message(&mut self, message: Self::SMRMessage) -> Result<Vec<Message<T>>>;
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
            bind_address: bind_address.unwrap(),
            total_nodes: other_nodes.len() as u32,
            other_nodes,
        })
    }
}

// TODO: revisit. Implement them on MaxAcceptedResponse
pub trait CommandTrait: Clone + Debug + Eq + PartialEq + Hash {}

impl<T> CommandTrait for T where T: Clone + Debug + Eq + PartialEq + Hash + Send {}

// Define a helper trait
pub trait SerializableCommand: CommandTrait + Serialize + for<'a> Deserialize<'a> {}
// Implement it automatically for all types satisfying the bounds
impl<T> SerializableCommand for T where T: CommandTrait + Serialize + for<'a> Deserialize<'a> {}

pub enum SmrMessage<S>
where
    S: StateMachine,
{
    PaxosMessage {
        message: Message<S::Command>,
    },
    ClientRequest {
        cmd: S::Command,
        sender: oneshot::Sender<<S as StateMachine>::Output>,
    },
}

pub struct SmrRuntime<S: StateMachine + 'static> {
    config: SmrConfig,
    algorithm: MultiPaxosNode<S::Command>,
    state_machine: S,
    channel: mpsc::Sender<SmrMessage<S>>,
    incoming_messages: mpsc::Receiver<SmrMessage<S>>,
    pending_proposals: HashMap<u64, (S::Command, Option<oneshot::Sender<S::Output>>)>,
    next_proposal_id: u64,
    other_node_streams: HashMap<u32, mpsc::Sender<Message<S::Command>>>,
}

impl<S> SmrRuntime<S>
where
    S: StateMachine,
{
    pub fn new(config: SmrConfig, state_machine: S, node_id: u32) -> Result<Self> {
        let algorithm = MultiPaxosNode::new(node_id, config.clone());
        let (tx, rx) = mpsc::channel(1024); // Single channel!
        Ok(SmrRuntime {
            config,
            algorithm,
            state_machine,
            channel: tx,
            incoming_messages: rx,
            pending_proposals: HashMap::new(),
            next_proposal_id: 0,
            other_node_streams: HashMap::new(),
        })
    }

    pub async fn run(&mut self) -> Result<()>
    where
        <S as StateMachine>::Command: Send, // Add trait bounds
    {
        let listener = TcpListener::bind(self.config.bind_address.clone()).await?;
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let mut stream = stream; // Make stream mutable
                loop {
                    let mut buf = Vec::new(); // Create a buffer to read into
                    match stream.read_buf(&mut buf).await {
                        Ok(0) => break, // Connection closed
                        Ok(_) => {
                            // Deserialize the message directly from the buffer
                            let message: Message<S::Command> =
                                serde_json::from_slice(&buf).unwrap(); // Or your preferred deserialization method
                                                                       //tx.send(SmrMessage::PaxosMessage { message }).await.unwrap();
                                                                       // Send the message
                        }
                        Err(e) => {
                            eprintln!("Error reading from stream: {}", e);
                            break;
                        }
                    }
                }
            }
        });
        info!("Listening on {}", self.config.bind_address);
        let tx = self.channel.clone(); // Clone the sender
                                       // 1. Establish connections to other nodes

        // inputs sides to send messages to tasks that forward to other nodes
        let mut other_node_streams: HashMap<u32, mpsc::Sender<Message<S::Command>>> =
            HashMap::new();

        for node_id in self.config.other_nodes.clone() {
            // Assuming port offset is 8080
            let node_id = node_id.parse::<u32>().unwrap();
            info!("Connecting to node {} at localhost:{}", node_id, node_id);
            let mut stream = TcpStream::connect(format!("localhost:{}", node_id))
                .await
                .unwrap();
            let (tx, mut rx) = mpsc::channel(1024);
            other_node_streams.insert(node_id, tx); // Store the sender

            // Spawn a task to receive messages from this node
            let incoming_messages = self.channel.clone();
            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    let buf = serde_json::to_vec(&msg).unwrap();
                    //incoming_messages.send(buf).await.unwrap();
                }
            });

            // Spawn a task to send messages to this node
            /*tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    let bytes = serde_json::to_vec(&msg).unwrap();
                    stream.write_all(&bytes).await.unwrap();
                }
            });*/
        }

        // Main loop: process messages from the single channel
        while let Some(msg) = self.incoming_messages.recv().await {
            /* match msg {
                SmrMessage::PaxosMessage { message } => {
                    let outgoing_messages = self.algorithm.handle_message(message)?; // Pass to Paxos
                    for msg in outgoing_messages {
                        match msg.kind() {
                            MessageKind::LearnedCommand { cmd } => {
                                let output = self.state_machine.apply(cmd.clone()).unwrap();

                                // Send the result back to the client
                                if let Some((_, Some(sender))) =
                                    self.pending_proposals.remove(&message.instance_id())
                                {
                                    // Use instance_id
                                    sender.send(output).unwrap_or_else(|_| {
                                        // Handle send error if the receiver has dropped
                                        eprintln!(
                                            "Failed to send result to client. Receiver dropped."
                                        );
                                    });
                                }
                            }
                            _ => {
                                // Broadcast initial Paxos messages to all other nodes
                                for addr in &self.config.other_nodes {
                                    // Serialize the message
                                    let bytes = serde_json::to_vec(&msg).unwrap();
                                    self.channel
                                        .send(SmrMessage::PaxosMessage { message: msg })
                                        .await
                                        .unwrap();
                                }
                            }
                        }
                    }
                }
                SmrMessage::ClientRequest { cmd, sender } => {
                    let proposal_id = self.next_proposal_id;
                    self.next_proposal_id += 1;
                    self.pending_proposals
                        .insert(proposal_id, (cmd.clone(), Some(sender))); // Store sender with proposal ID

                    let outgoing_messages = self.algorithm.propose(cmd)?; // Propose through MultiPaxos

                    for msg in outgoing_messages {
                        self.channel
                            .send(SmrMessage::PaxosMessage { message: msg })
                            .await
                            .unwrap();
                    }
                }
            }*/
        }
        Ok(())
    }
}
