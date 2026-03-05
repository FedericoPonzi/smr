use crate::SerializableCommand;
use crate::multipaxos::Ballot;
use crate::storage::Storage;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

/// Persisted acceptor state for a single Paxos instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptorState<C> {
    pub max_ballot: Ballot,
    pub max_accepted: Option<AcceptedValue<C>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptedValue<C> {
    pub ballot: Ballot,
    pub command: C,
}

/// Paxos-aware storage layer wrapping a generic `Storage` backend.
pub struct PaxosStorage<C> {
    storage: Box<dyn Storage>,
    _phantom: PhantomData<C>,
}

impl<C: SerializableCommand> PaxosStorage<C> {
    pub fn new(storage: Box<dyn Storage>) -> Self {
        Self {
            storage,
            _phantom: PhantomData,
        }
    }

    pub fn save_promise(&mut self, instance_id: u64, ballot: Ballot) -> anyhow::Result<()> {
        let key = format!("acceptor/{}/ballot", instance_id);
        let value = serde_json::to_vec(&ballot)?;
        self.storage.put(key.as_bytes(), &value)
    }

    pub fn save_accept(&mut self, instance_id: u64, ballot: Ballot, cmd: &C) -> anyhow::Result<()> {
        self.save_promise(instance_id, ballot)?;
        let key = format!("acceptor/{}/accepted", instance_id);
        let value = serde_json::to_vec(&AcceptedValue {
            ballot,
            command: cmd.clone(),
        })?;
        self.storage.put(key.as_bytes(), &value)
    }

    pub fn load_acceptor_state(
        &self,
        instance_id: u64,
    ) -> anyhow::Result<Option<AcceptorState<C>>> {
        let ballot_key = format!("acceptor/{}/ballot", instance_id);
        let Some(ballot_bytes) = self.storage.get(ballot_key.as_bytes())? else {
            return Ok(None);
        };
        let max_ballot: Ballot = serde_json::from_slice(&ballot_bytes)?;

        let accepted_key = format!("acceptor/{}/accepted", instance_id);
        let max_accepted = match self.storage.get(accepted_key.as_bytes())? {
            Some(bytes) => Some(serde_json::from_slice(&bytes)?),
            None => None,
        };

        Ok(Some(AcceptorState {
            max_ballot,
            max_accepted,
        }))
    }

    pub fn load_all_acceptor_states(&self) -> anyhow::Result<Vec<(u64, AcceptorState<C>)>> {
        let entries = self.storage.scan_prefix(b"acceptor/")?;
        // Collect unique instance IDs from keys like "acceptor/{id}/ballot"
        let mut instance_ids: Vec<u64> = entries
            .iter()
            .filter_map(|(k, _)| {
                let s = std::str::from_utf8(k).ok()?;
                let parts: Vec<&str> = s.split('/').collect();
                if parts.len() >= 2 {
                    parts[1].parse().ok()
                } else {
                    None
                }
            })
            .collect();
        instance_ids.sort_unstable();
        instance_ids.dedup();

        let mut result = Vec::new();
        for id in instance_ids {
            if let Some(state) = self.load_acceptor_state(id)? {
                result.push((id, state));
            }
        }
        Ok(result)
    }
}

/// Append-only command log wrapping a generic `Storage` backend.
pub struct CommandLog<C> {
    storage: Box<dyn Storage>,
    _phantom: PhantomData<C>,
}

impl<C: SerializableCommand> CommandLog<C> {
    pub fn new(storage: Box<dyn Storage>) -> Self {
        Self {
            storage,
            _phantom: PhantomData,
        }
    }

    pub fn append(&mut self, instance_id: u64, command: &C) -> anyhow::Result<()> {
        let key = format!("log/{:020}", instance_id);
        let value = serde_json::to_vec(command)?;
        self.storage.put(key.as_bytes(), &value)
    }

    pub fn last_applied(&self) -> anyhow::Result<u64> {
        match self.storage.get(b"meta/last_applied")? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Ok(0),
        }
    }

    pub fn set_last_applied(&mut self, id: u64) -> anyhow::Result<()> {
        let value = serde_json::to_vec(&id)?;
        self.storage.put(b"meta/last_applied", &value)
    }

    pub fn replay(&self) -> anyhow::Result<Vec<(u64, C)>> {
        let entries = self.storage.scan_prefix(b"log/")?;
        let mut result = Vec::new();
        for (key, value) in entries {
            let key_str = std::str::from_utf8(&key)?;
            let id: u64 = key_str
                .strip_prefix("log/")
                .ok_or_else(|| anyhow::anyhow!("invalid log key"))?
                .parse()?;
            let command: C = serde_json::from_slice(&value)?;
            result.push((id, command));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::tests::SharedStorage;
    use crate::storage::{InMemoryStorage, Storage};
    #[test]
    fn test_paxos_storage_save_load_promise() {
        let s = InMemoryStorage::new();
        let mut ps: PaxosStorage<u32> = PaxosStorage::new(Box::new(s));
        assert!(ps.load_acceptor_state(0).unwrap().is_none());

        ps.save_promise(0, 5).unwrap();
        let state = ps.load_acceptor_state(0).unwrap().unwrap();
        assert_eq!(state.max_ballot, 5);
        assert!(state.max_accepted.is_none());
    }

    #[test]
    fn test_paxos_storage_save_load_accept() {
        let s = InMemoryStorage::new();
        let mut ps: PaxosStorage<String> = PaxosStorage::new(Box::new(s));

        ps.save_accept(1, 3, &"hello".to_string()).unwrap();
        let state = ps.load_acceptor_state(1).unwrap().unwrap();
        assert_eq!(state.max_ballot, 3);
        let acc = state.max_accepted.unwrap();
        assert_eq!(acc.ballot, 3);
        assert_eq!(acc.command, "hello");
    }

    #[test]
    fn test_paxos_storage_load_all() {
        let s = InMemoryStorage::new();
        let mut ps: PaxosStorage<u32> = PaxosStorage::new(Box::new(s));

        ps.save_promise(0, 1).unwrap();
        ps.save_accept(2, 5, &42).unwrap();

        let all = ps.load_all_acceptor_states().unwrap();
        assert_eq!(all.len(), 2);
        // Sorted by instance_id due to BTreeMap
        assert_eq!(all[0].0, 0);
        assert_eq!(all[1].0, 2);
    }

    #[test]
    fn test_command_log_append_and_replay() {
        let s = InMemoryStorage::new();
        let mut log: CommandLog<String> = CommandLog::new(Box::new(s));

        assert_eq!(log.last_applied().unwrap(), 0);
        assert!(log.replay().unwrap().is_empty());

        log.append(0, &"cmd_a".to_string()).unwrap();
        log.append(1, &"cmd_b".to_string()).unwrap();
        log.set_last_applied(2).unwrap();

        assert_eq!(log.last_applied().unwrap(), 2);
        let entries = log.replay().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (0, "cmd_a".to_string()));
        assert_eq!(entries[1], (1, "cmd_b".to_string()));
    }

    /// Recovery test: populate storage, then construct new PaxosStorage/CommandLog from same
    /// storage and verify state survived.
    #[test]
    fn test_recovery_round_trip() {
        // Shared storage simulating persistent disk
        let storage = std::sync::Arc::new(std::sync::Mutex::new(InMemoryStorage::new()));

        // Phase 1: "First run" — write some state
        {
            let s1: Box<dyn Storage> = Box::new(SharedStorage(std::sync::Arc::clone(&storage)));
            let s2: Box<dyn Storage> = Box::new(SharedStorage(std::sync::Arc::clone(&storage)));

            let mut ps: PaxosStorage<u32> = PaxosStorage::new(s1);
            ps.save_promise(0, 5).unwrap();
            ps.save_accept(1, 10, &42).unwrap();

            let mut cl: CommandLog<u32> = CommandLog::new(s2);
            cl.append(0, &100).unwrap();
            cl.append(1, &200).unwrap();
            cl.set_last_applied(2).unwrap();
        }
        // PaxosStorage and CommandLog dropped — simulates crash

        // Phase 2: "Recovery" — new instances, same backing storage
        {
            let s1: Box<dyn Storage> = Box::new(SharedStorage(std::sync::Arc::clone(&storage)));
            let s2: Box<dyn Storage> = Box::new(SharedStorage(std::sync::Arc::clone(&storage)));

            let ps: PaxosStorage<u32> = PaxosStorage::new(s1);
            let all = ps.load_all_acceptor_states().unwrap();
            assert_eq!(all.len(), 2);
            assert_eq!(all[0].0, 0);
            assert_eq!(all[0].1.max_ballot, 5);
            assert!(all[0].1.max_accepted.is_none());
            assert_eq!(all[1].0, 1);
            assert_eq!(all[1].1.max_ballot, 10);
            assert_eq!(all[1].1.max_accepted.as_ref().unwrap().command, 42);

            let cl: CommandLog<u32> = CommandLog::new(s2);
            assert_eq!(cl.last_applied().unwrap(), 2);
            let entries = cl.replay().unwrap();
            assert_eq!(entries, vec![(0, 100), (1, 200)]);
        }
    }
}
