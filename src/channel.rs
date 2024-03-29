use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

use crate::multipaxos::MessageKind;

pub trait Channel: SenderChannel {
    fn receive(&mut self) -> anyhow::Result<Option<MessageKind>>;
}

pub trait SenderChannel {
    fn send(&mut self, message: MessageKind) -> anyhow::Result<()>;
}

pub(crate) struct SharedMemoryChannel<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

struct Inner<T> {
    data: VecDeque<T>,
    senders: Vec<Sender<T>>,
    receivers: Vec<Receiver<T>>,
}

impl<T> SharedMemoryChannel<T> {
    // Create a new shared memory channel
    pub fn new() -> Self {
        let inner = Inner {
            data: VecDeque::new(),
            senders: Vec::new(),
            receivers: Vec::new(),
        };
        let arc_inner = Arc::new(Mutex::new(inner));

        SharedMemoryChannel { inner: arc_inner }
    }

    // Get a sender and receiver pair for a new endpoint
    fn get_ends(&mut self) -> (Sender<T>, Receiver<T>) {
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut inner = self.inner.lock().unwrap();
        inner.senders.push(sender.clone());
        //inner.receivers.push(receiver.clone());

        // Send any buffered data to the new receiver
        /*for data in inner.data.iter() {
            sender.send(data.clone()).unwrap();
        }*/

        (sender, receiver)
    }
}

impl<T> SenderChannel for SharedMemoryChannel<T> {
    fn send(&mut self, message: MessageKind) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<T> Channel for SharedMemoryChannel<T> {
    // Send data to all receivers
    // Receive data from the channel
    fn receive(&mut self) -> anyhow::Result<Option<MessageKind>> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(receiver) = inner.receivers.first() {
            Ok(None)
            //Ok(receiver.recv().ok())
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn test_multiple_senders_receivers() {
        let mut channel = SharedMemoryChannel::new();
        let (sender1, receiver1) = channel.get_ends();
        let (sender2, receiver2) = channel.get_ends();

        // Spawn a thread to simulate sender 1
        thread::spawn(move || {
            for i in 0..5 {
                sender1.send(i).unwrap();
                thread::sleep(std::time::Duration::from_secs(1));
            }
        });

        // Spawn another thread to simulate sender 2
        thread::spawn(move || {
            for i in 5..10 {
                sender2.send(i).unwrap();
                thread::sleep(std::time::Duration::from_secs(1));
            }
        });

        // Spawn a receiver thread
        let received_data1: Vec<_> = thread::spawn(move || {
            let mut data = Vec::new();
            for _ in 0..5 {
                if let Some(received_data) = receiver1.recv().ok() {
                    data.push(received_data);
                }
            }
            data
        })
        .join()
        .unwrap();

        // Spawn another receiver thread
        let received_data2: Vec<_> = thread::spawn(move || {
            let mut data = Vec::new();
            for _ in 0..5 {
                if let Some(received_data) = receiver2.recv().ok() {
                    data.push(received_data);
                }
            }
            data
        })
        .join()
        .unwrap();

        // Check if all data was received correctly
        assert_eq!(received_data1, vec![0, 1, 2, 3, 4]);
        assert_eq!(received_data2, vec![5, 6, 7, 8, 9]);
    }
}
