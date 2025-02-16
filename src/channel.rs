//! Got it! Here’s the updated breakdown:
//!
//!  ### `Channel` Struct Responsibilities:
//!
//!  1. **Exposes a single sender and receiver** to `SmrRuntime`:
//!    - Allows sending messages to all connected nodes.
//!    - Allows receiving messages from other nodes.
//!
//! 2. **Manages network connections** based on node ID:
//!    - Binds to a specified local address at startup.
//!    - Initiates **outgoing** connections **only** to nodes with a **higher ID**.
//!    - If the node has the **highest ID**, it connects to the node with the **lowest ID**.
//!    - Accepts **incoming** connections from any node.
//!
//! 3. **Maintains a connection map**:
//!    - Keeps track of active connections for querying.
//!
//! 4. **Handles failures gracefully**:
//!    - If a connection fails or closes, waits for a random timeout and retries.
//!    - Continues retrying until the connection is restored.
//!
//! This ensures a structured connection setup where each node initiates
//! connections in a **deterministic way**, reducing redundant connection attempts.

use crate::multipaxos::Message;
use crate::{Channel, Result, SmrMessage};
use log::warn;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::sync::mpsc::{Receiver, Sender};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::io::BufReader;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex},
    task,
};
use tracing::{debug, error, info};

#[derive(Debug, Serialize, Deserialize)]
pub struct TcpMessage<T> {
    pub sender_id: u32,
    pub payload: T,
}

type Connections = Arc<Mutex<HashMap<u32, OwnedWriteHalf>>>;

/// First message: the id of the sender's node. The receiver stores the sender's id in connections map.
/// Then every message is a prefix of len + the T struct.
/// sender and receiver are the interfaces to this channel. Sender can be used to send a message to
/// all the connected nodes.
/// receiver is used to read messages sent by all the other nodes.
pub struct TcpChannel<T: Serialize + for<'a> Deserialize<'a> + Send + 'static> {
    id: u32,
    local_addr: SocketAddr,
    peers: Vec<SocketAddr>,
    connections: Connections,
    // interface with internal runtime
    sender: mpsc::Sender<T>,
    receiver: mpsc::Receiver<T>,
}

impl<T> TcpChannel<T>
where
    T: Serialize + for<'a> Deserialize<'a> + Send + Sync + 'static + Debug,
{
    pub async fn new(id: u32, local_addr: SocketAddr, peers: Vec<(u32, SocketAddr)>) -> Self {
        let (tx, rx) = mpsc::channel(100);
        let connections = Arc::new(Mutex::new(HashMap::new()));

        let peer_map: HashMap<u32, SocketAddr> = peers.into_iter().collect();

        let channel = Self {
            id,
            local_addr,
            peers: peer_map.values().cloned().collect(),
            connections: connections.clone(),
            sender: tx.clone(),
            receiver: rx,
        };

        channel.start_listener(connections.clone()).await;
        channel.initiate_connections(peer_map, connections).await;

        channel
    }

    /// Starts listening for incoming connections.
    async fn start_listener(&self, connections: Connections) {
        let listener = TcpListener::bind(self.local_addr)
            .await
            .expect("Failed to bind socket");
        info!("Node {} listening on {}", self.id, self.local_addr);

        let id = self.id;
        let sender = self.sender.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        debug!("Node {} accepted connection from {}", id, addr);

                        // Disable Nagle's algorithm (set TCP_NODELAY)
                        stream.set_nodelay(true).unwrap();

                        let (mut reader, writer) = stream.into_split();
                        debug!("Node {} reading id", id);
                        let peer_id = match reader.read_u32().await {
                            Ok(peer_id) => peer_id,
                            Err(_) => {
                                error!("Node {} failed to read peer ID from {}", id, addr);
                                continue;
                            }
                        };
                        debug!(
                            "Node {} received id: {}, already exists? {} ",
                            id,
                            peer_id,
                            connections.lock().await.contains_key(&peer_id)
                        );
                        connections.lock().await.insert(peer_id, writer);
                        Self::start_reading(id, sender.clone(), peer_id, reader).await;
                        debug!("Node {} added {} to connection map", id, peer_id);
                    }
                    Err(e) => error!("Error accepting connection: {:?}", e),
                }
            }
        });
    }

    /// Initiates outgoing connections to higher ID nodes or wraps around to the lowest.
    async fn initiate_connections(
        &self,
        peer_map: HashMap<u32, SocketAddr>,
        connections: Connections,
    ) {
        let mut targets: Vec<_> = peer_map
            .clone()
            .into_iter()
            .filter(|(peer_id, _)| *peer_id > self.id)
            .collect();
        debug!("Node {} - targets: {:?}", self.id, targets);

        let id = self.id;
        for (peer_id, addr) in targets.into_iter() {
            debug!("Node {} connecting to {}", id, peer_id);
            let connections_clone = connections.clone();
            let sender_cl = self.sender.clone();
            task::spawn(async move {
                loop {
                    match TcpStream::connect(addr).await {
                        Ok(stream) => {
                            // Disable Nagle's algorithm (set TCP_NODELAY)
                            stream.set_nodelay(true).unwrap();

                            let (reader, writer) = stream.into_split();
                            debug!("Node {} connected to {}", id, peer_id);
                            // send our id out

                            Self::send_id(id, peer_id, connections_clone.clone(), writer).await;
                            Self::start_reading(id, sender_cl.clone(), peer_id, reader).await;
                            connections_clone.clone().lock().await.remove(&peer_id);
                            debug!("Node {}: for some reason stopped reading, retrying...", id);
                        }
                        Err(e) => {
                            warn!("Node {} failed to connect to {}: {:?}", id, peer_id, e);
                        }
                    }

                    let wait_time = rand::rng().random_range(50..500);
                    info!(
                        "Node {} retrying connection to {} in {} ms",
                        id, peer_id, wait_time
                    );
                    tokio::time::sleep(Duration::from_millis(wait_time)).await;
                }
            });
        }
    }
    async fn send_id(id: u32, peer_id: u32, conn: Connections, mut writer: OwnedWriteHalf) {
        debug!("Node {} sending id to {}", id, peer_id);
        if writer.write_u32(id).await.is_ok() {
            conn.lock().await.insert(peer_id, writer);
        } else {
            error!("Node {} failed to send its id to {}", id, peer_id);
        }
    }
    async fn read_one_struct(reader: &mut OwnedReadHalf) -> Result<T> {
        let mut reader = reader; //BufReader::new(reader);

        // Read exactly 4 bytes for the length prefix
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let msg_len = u32::from_le_bytes(len_buf) as usize;
        debug!("read_one_struct() - msg_len: {}", msg_len);
        // Read the actual message
        let mut msg_buf = vec![0u8; msg_len];
        reader.read_exact(&mut msg_buf).await?;

        // Deserialize into T
        let message: T = bincode::deserialize(&msg_buf)?;
        debug!("read_one_struct() - message: {:?}", message);
        Ok(message)
    }
    // read from reader and write to sender
    async fn start_reading(
        id: u32,
        sender: mpsc::Sender<T>,
        peer_id: u32,
        mut reader: OwnedReadHalf,
    ) {
        debug!("{}: start_reading() called for {} peer", id, peer_id);
        //TODO: return joinHandle
        loop {
            // Read message
            let msg: T = Self::read_one_struct(&mut reader)
                .await
                .expect("Failed to read message");

            // Send message to the channel
            if sender.send(msg).await.is_err() {
                error!("Receiver dropped; stopping read loop for node {}", peer_id);
                break;
            }
        }
    }

    pub async fn send(&self, message: T) {
        debug!(
            "Node {}: send() called, for message: '{:?}'",
            self.id, message
        );

        // Serialize the message with length prefix
        let serialized = match bincode::serialize(&message) {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to serialize message: {:?}", e);
                return;
            }
        };

        let msg_len = (serialized.len() as u32).to_le_bytes(); // Convert length to 4 bytes

        let mut connections = self.connections.lock().await;

        for (&peer_id, stream) in connections.iter_mut() {
            debug!("Sending message to {}", peer_id);
            if let Err(e) = stream.write_all(&msg_len).await {
                error!("Failed to send message length to {}: {:?}", peer_id, e);
                continue;
            }
            if let Err(e) = stream.write_all(&serialized).await {
                error!("Failed to send message to {}: {:?}", peer_id, e);
                continue;
            }
            if let Err(e) = stream.flush().await {
                error!("Failed to flush stream to {}: {:?}", peer_id, e);
            }
        }
    }

    /// Receives a message from any peer.
    pub async fn receive(&mut self) -> Option<T> {
        self.receiver.recv().await
    }

    /// Returns a list of currently connected peers.
    pub async fn connected_peers(&self) -> Vec<u32> {
        self.connections.lock().await.keys().cloned().collect()
    }
}

pub struct SharedMemoryChannel<C>
where
    C: Serialize + for<'a> Deserialize<'a> + Clone,
{
    inner: Arc<std::sync::Mutex<Inner<C>>>,
}

struct Inner<C>
where
    C: Serialize + for<'a> Deserialize<'a>,
{
    data: VecDeque<C>,
    senders: Vec<Sender<C>>,
}

impl<C> SharedMemoryChannel<C>
where
    C: Serialize + for<'a> Deserialize<'a> + Clone,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(Inner {
                data: VecDeque::new(),
                senders: Vec::new(),
            })),
        }
    }
    // Get a sender and receiver pair for a new endpoint
    pub fn get_ends(&self) -> (Sender<C>, Receiver<C>) {
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut inner = self.inner.lock().unwrap();

        inner.senders.push(sender.clone());

        // Send any buffered data to the new receiver
        for data in &inner.data {
            if sender.send(data.clone()).is_err() {
                break; // If sending fails, stop (receiver may be dropped)
            }
        }

        (sender, receiver)
    }
}

impl<C> Channel<C> for SharedMemoryChannel<C>
where
    C: Serialize + for<'a> Deserialize<'a> + Clone,
{
    fn receive(&mut self) -> Result<Option<C>> {
        let mut inner = self.inner.lock().unwrap();
        Ok(inner.data.pop_front())
    }

    fn send(&mut self, message: C) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.data.push_back(message);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn test_multiple_senders_receivers() {
        use std::sync::{Arc, Mutex};

        let channel = SharedMemoryChannel::new();
        let (sender1, receiver1) = channel.get_ends();
        let (sender2, receiver2) = channel.get_ends();

        let received_data = Arc::new(Mutex::new(Vec::new()));

        // Spawn sender 1
        let sender1_thread = {
            thread::spawn(move || {
                for i in 0..5 {
                    sender1.send(i).unwrap();
                    thread::sleep(std::time::Duration::from_millis(100));
                }
            })
        };

        // Spawn sender 2
        let sender2_thread = {
            thread::spawn(move || {
                for i in 5..10 {
                    sender2.send(i).unwrap();
                    thread::sleep(std::time::Duration::from_millis(100));
                }
            })
        };

        // Receiver threads
        let receiver1_thread = {
            let received_data = Arc::clone(&received_data);
            thread::spawn(move || {
                for _ in 0..5 {
                    if let Some(d) = receiver1.recv().ok() {
                        received_data.lock().unwrap().push(d);
                    }
                }
            })
        };

        let receiver2_thread = {
            let received_data = Arc::clone(&received_data);
            thread::spawn(move || {
                for _ in 0..5 {
                    if let Some(d) = receiver2.recv().ok() {
                        received_data.lock().unwrap().push(d);
                    }
                }
            })
        };

        // Ensure all threads complete
        sender1_thread.join().unwrap();
        sender2_thread.join().unwrap();
        receiver1_thread.join().unwrap();
        receiver2_thread.join().unwrap();

        // Collect received messages
        let mut received = received_data.lock().unwrap().clone();
        received.sort();

        // Check if all messages are received
        assert_eq!(received, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }
}
