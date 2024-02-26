pub trait Channel {
    fn send_message() -> anyhow::Result<()>;
    fn receive_message<F>(func: F) -> anyhow::Result<()>
    where
        F: FnOnce() -> String;
}

pub struct SharedMemoryChannel<M> {
    buffer: Vec<M>,
}
impl <M> SharedMemoryChannel<M> {
    pub fn new() -> Self {
        SharedMemoryChannel { buffer: vec![] }
    }
}
impl <M> Default for SharedMemoryChannel<M> {
    fn default() -> Self {
        Self::new()
    }
}
impl <M> Channel for SharedMemoryChannel<M> {
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
