use serde::{Deserialize, Serialize};
use std::sync::RwLock;
use std::{collections::HashMap, sync::Arc};

use smr::StateMachine;

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

impl StateMachine for InnerStateMachine {
    type Command = Command; // Use the renamed Command
    type Output = Result<Option<String>, String>;

    fn apply(&mut self, command: Self::Command) -> Self::Output {
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
    inner: Arc<RwLock<InnerStateMachine>>,
}

impl KeyValueStore {
    pub(crate) async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        // Made async
        let inner = self.inner.read().await; // Use .await
        Ok(inner.values.get(key).cloned())
    }

    pub(crate) async fn set(&self, key: &str, value: &str) -> anyhow::Result<()> {
        // Made async
        let mut inner = self.inner.write().await; // Use .await
        inner.values.insert(key.to_string(), value.to_string());
        Ok(())
    }
}

impl Default for KeyValueStore {
    fn default() -> KeyValueStore {
        KeyValueStore {
            inner: Arc::new(RwLock::new(Inner {
                values: HashMap::default(),
            })),
        }
    }
}
