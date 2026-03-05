//! Generic storage interface and implementations.

use anyhow::Result;
use std::collections::BTreeMap;

/// A backend-agnostic key-value storage interface.
pub trait Storage: Send + Sync {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn delete(&mut self, key: &[u8]) -> Result<()>;
    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

/// In-memory storage backed by a BTreeMap (sorted for prefix scans).
pub struct InMemoryStorage {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for InMemoryStorage {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(key).cloned())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.data.remove(key);
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .data
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
}

/// Persistent storage backed by sled (an embedded database).
#[cfg(feature = "sled")]
pub struct SledStorage {
    db: sled::Db,
}

#[cfg(feature = "sled")]
impl SledStorage {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }
}

#[cfg(feature = "sled")]
impl Storage for SledStorage {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.insert(key, value)?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key)?.map(|v| v.to_vec()))
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.db.remove(key)?;
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .db
            .scan_prefix(prefix)
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect())
    }
}

/// Persistent storage backed by RocksDB.
#[cfg(feature = "rocksdb")]
pub struct RocksDbStorage {
    db: rocksdb::DB,
}

#[cfg(feature = "rocksdb")]
impl RocksDbStorage {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let db = rocksdb::DB::open_default(path)?;
        Ok(Self { db })
    }
}

#[cfg(feature = "rocksdb")]
impl Storage for RocksDbStorage {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(true);
        self.db.put_opt(key, value, &opts)?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key)?)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.db.delete(key)?;
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let iter = self.db.prefix_iterator(prefix);
        let mut results = Vec::new();
        for item in iter {
            let (k, v) = item?;
            if !k.starts_with(prefix) {
                break;
            }
            results.push((k.to_vec(), v.to_vec()));
        }
        Ok(results)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Wrapper that delegates to a shared InMemoryStorage behind Arc<Mutex>.
    pub(crate) struct SharedStorage(pub(crate) std::sync::Arc<std::sync::Mutex<InMemoryStorage>>);

    impl Storage for SharedStorage {
        fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
            self.0.lock().unwrap().put(key, value)
        }
        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.0.lock().unwrap().get(key)
        }
        fn delete(&mut self, key: &[u8]) -> Result<()> {
            self.0.lock().unwrap().delete(key)
        }
        fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.0.lock().unwrap().scan_prefix(prefix)
        }
    }

    #[test]
    fn test_put_and_get() {
        let mut s = InMemoryStorage::new();
        s.put(b"key1", b"val1").unwrap();
        assert_eq!(s.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(s.get(b"key2").unwrap(), None);
    }

    #[test]
    fn test_put_overwrites() {
        let mut s = InMemoryStorage::new();
        s.put(b"k", b"v1").unwrap();
        s.put(b"k", b"v2").unwrap();
        assert_eq!(s.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_delete() {
        let mut s = InMemoryStorage::new();
        s.put(b"k", b"v").unwrap();
        s.delete(b"k").unwrap();
        assert_eq!(s.get(b"k").unwrap(), None);
    }

    #[test]
    fn test_delete_nonexistent() {
        let mut s = InMemoryStorage::new();
        s.delete(b"nope").unwrap(); // should not error
    }

    #[test]
    fn test_scan_prefix() {
        let mut s = InMemoryStorage::new();
        s.put(b"acceptor/0/ballot", b"1").unwrap();
        s.put(b"acceptor/1/ballot", b"2").unwrap();
        s.put(b"acceptor/1/accepted", b"3").unwrap();
        s.put(b"log/0", b"cmd").unwrap();

        let results = s.scan_prefix(b"acceptor/1/").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"acceptor/1/accepted");
        assert_eq!(results[1].0, b"acceptor/1/ballot");

        let all_acceptor = s.scan_prefix(b"acceptor/").unwrap();
        assert_eq!(all_acceptor.len(), 3);

        let empty = s.scan_prefix(b"nonexistent/").unwrap();
        assert!(empty.is_empty());
    }

    #[cfg(feature = "sled")]
    mod sled_tests {
        use super::super::*;
        use crate::multipaxos::storage::{CommandLog, PaxosStorage};

        fn temp_sled() -> SledStorage {
            let dir = tempfile::tempdir().unwrap();
            SledStorage::open(dir.path()).unwrap()
        }

        #[test]
        fn test_sled_put_get_delete() {
            let mut s = temp_sled();
            s.put(b"k1", b"v1").unwrap();
            assert_eq!(s.get(b"k1").unwrap(), Some(b"v1".to_vec()));
            assert_eq!(s.get(b"k2").unwrap(), None);
            s.delete(b"k1").unwrap();
            assert_eq!(s.get(b"k1").unwrap(), None);
        }

        #[test]
        fn test_sled_scan_prefix() {
            let mut s = temp_sled();
            s.put(b"acceptor/0/ballot", b"1").unwrap();
            s.put(b"acceptor/1/ballot", b"2").unwrap();
            s.put(b"log/0", b"x").unwrap();

            let results = s.scan_prefix(b"acceptor/").unwrap();
            assert_eq!(results.len(), 2);
            assert!(s.scan_prefix(b"log/").unwrap().len() == 1);
        }

        #[test]
        fn test_sled_paxos_storage_round_trip() {
            let s = temp_sled();
            let mut ps: PaxosStorage<String> = PaxosStorage::new(Box::new(s));

            ps.save_accept(0, 7, &"hello".to_string()).unwrap();
            let state = ps.load_acceptor_state(0).unwrap().unwrap();
            assert_eq!(state.max_ballot, 7);
            assert_eq!(state.max_accepted.unwrap().command, "hello");
        }

        #[test]
        fn test_sled_command_log_round_trip() {
            let s = temp_sled();
            let mut cl: CommandLog<u32> = CommandLog::new(Box::new(s));

            cl.append(0, &10).unwrap();
            cl.append(1, &20).unwrap();
            cl.set_last_applied(2).unwrap();

            assert_eq!(cl.last_applied().unwrap(), 2);
            let entries = cl.replay().unwrap();
            assert_eq!(entries, vec![(0, 10), (1, 20)]);
        }

        #[test]
        fn test_sled_survives_reopen() {
            let dir = tempfile::tempdir().unwrap();

            // Write data
            {
                let mut s = SledStorage::open(dir.path()).unwrap();
                s.put(b"acceptor/0/ballot", &serde_json::to_vec(&5u32).unwrap())
                    .unwrap();
            }
            // Reopen and verify
            {
                let s = SledStorage::open(dir.path()).unwrap();
                let val = s.get(b"acceptor/0/ballot").unwrap().unwrap();
                let ballot: u32 = serde_json::from_slice(&val).unwrap();
                assert_eq!(ballot, 5);
            }
        }
    }
}
