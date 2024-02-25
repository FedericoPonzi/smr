use crate::Message;

pub trait Channel {
    fn send_message() -> anyhow::Result<()>;
    fn receive_message<F>(func: F) -> anyhow::Result<()>
    where
        F: FnOnce() -> String;
}

pub struct SharedMemoryChannel {
    buffer: Vec<Message>,
}
impl SharedMemoryChannel {
    pub fn new() -> Self {
        SharedMemoryChannel { buffer: vec![] }
    }
}
impl Default for SharedMemoryChannel {
    fn default() -> Self {
        Self::new()
    }
}
impl Channel for SharedMemoryChannel {
    fn send_message() -> anyhow::Result<()> {
        Ok(())
    }
    fn receive_message<F>(filter: F) -> anyhow::Result<()>
    where
        F: FnOnce() -> String,
    {
        Ok(())
    }
}
