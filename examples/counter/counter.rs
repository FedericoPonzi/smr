use serde::{Deserialize, Serialize};
use smr::{SmrRuntime, StateMachine};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Command {
    Increment,
    Decrement,
    Get,
}

pub struct CounterStateMachine {
    value: i64,
}

impl CounterStateMachine {
    pub fn new() -> Self {
        Self { value: 0 }
    }
}

impl StateMachine for CounterStateMachine {
    type Command = Command;
    type Output = i64;

    fn apply(&mut self, command: Self::Command) -> smr::Result<Self::Output> {
        match command {
            Command::Increment => {
                self.value += 1;
                Ok(self.value)
            }
            Command::Decrement => {
                self.value -= 1;
                Ok(self.value)
            }
            Command::Get => Ok(self.value),
        }
    }
}

#[derive(Clone)]
pub struct CounterStore {
    inner: Arc<RwLock<SmrRuntime<CounterStateMachine>>>,
}

impl CounterStore {
    pub fn new(runtime: SmrRuntime<CounterStateMachine>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(runtime)),
        }
    }

    pub(crate) async fn increment(&self) -> anyhow::Result<i64> {
        self.propose(Command::Increment).await
    }

    pub(crate) async fn decrement(&self) -> anyhow::Result<i64> {
        self.propose(Command::Decrement).await
    }

    pub(crate) async fn get(&self) -> anyhow::Result<i64> {
        self.propose(Command::Get).await
    }

    async fn propose(&self, command: Command) -> anyhow::Result<i64> {
        let mut lc = self.inner.write().await;
        let rx = lc.propose(command).await?;
        drop(lc);
        Ok(rx.await?)
    }
}
