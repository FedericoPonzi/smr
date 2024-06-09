//! Module that handles the persistence and durability of messages.
use std::io::Write;

use anyhow::Result;

pub struct AppendOnly<T: Write> {
    log_storage: T,
}

impl<T> AppendOnly<T>
where
    T: Write,
{
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.log_storage.write_all(data)?;
        Ok(())
    }
}
