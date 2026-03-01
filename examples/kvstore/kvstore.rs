use anyhow::bail;
use serde::{Deserialize, Serialize};
use smr::{SmrRuntime, StateMachine};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Command {
    // Renamed to Command (more generic)
    Get { key: String },
    Set { key: String, value: String },
}

pub struct InnerStateMachine {
    // TODO: adding persistence (e.g., sled) to survive restarts
    values: HashMap<String, String>,
}
impl InnerStateMachine {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }
}

impl StateMachine for InnerStateMachine {
    type Command = Command; // Use the renamed Command
    type Output = Option<String>;

    fn apply(&mut self, command: Self::Command) -> smr::Result<Self::Output> {
        match command {
            Command::Get { key } => Ok(self.values.get(&key).cloned()),
            Command::Set { key, value } => {
                self.values.insert(key, value);
                Ok(None)
            }
        }
    }
}

#[derive(Clone)]
pub struct KeyValueStore {
    inner: Arc<RwLock<SmrRuntime<InnerStateMachine>>>,
}

impl KeyValueStore {
    pub fn new(runtime: SmrRuntime<InnerStateMachine>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(runtime)),
        }
    }
    pub(crate) async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        let command = Command::Get {
            key: key.to_string(),
        };
        match self.propose(command).await {
            Ok(v) => {
                debug!("get result for key {:?} = {:?}", key, v);
                Ok(v)
            }
            Err(err) => {
                error!("get error: {:?}", err);
                bail!(err);
            }
        }
    }

    pub(crate) async fn set(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let command = Command::Set {
            key: key.to_string(),
            value: value.to_string(),
        };
        let _res = self.propose(command).await?;
        Ok(())
    }
    async fn propose(&self, command: Command) -> anyhow::Result<Option<String>> {
        let mut lc = self.inner.write().await;
        let rx = lc.propose(command).await?;
        drop(lc);
        Ok(rx.await?)
    }
}
