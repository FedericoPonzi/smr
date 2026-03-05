#![cfg(feature = "rocksdb")]

use smr::multipaxos::storage::{CommandLog, PaxosStorage};
use smr::storage::{RocksDbStorage, Storage};

fn temp_rocksdb() -> RocksDbStorage {
    let dir = tempfile::tempdir().unwrap();
    RocksDbStorage::open(dir.path()).unwrap()
}

#[test]
fn test_rocksdb_put_get_delete() {
    let mut s = temp_rocksdb();
    s.put(b"k1", b"v1").unwrap();
    assert_eq!(s.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(s.get(b"k2").unwrap(), None);
    s.delete(b"k1").unwrap();
    assert_eq!(s.get(b"k1").unwrap(), None);
}

#[test]
fn test_rocksdb_scan_prefix() {
    let mut s = temp_rocksdb();
    s.put(b"acceptor/0/ballot", b"1").unwrap();
    s.put(b"acceptor/1/ballot", b"2").unwrap();
    s.put(b"log/0", b"x").unwrap();
    let results = s.scan_prefix(b"acceptor/").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(s.scan_prefix(b"log/").unwrap().len(), 1);
}

#[test]
fn test_rocksdb_paxos_storage_round_trip() {
    let s = temp_rocksdb();
    let mut ps: PaxosStorage<String> = PaxosStorage::new(Box::new(s));
    ps.save_accept(0, 7, &"hello".to_string()).unwrap();
    let state = ps.load_acceptor_state(0).unwrap().unwrap();
    assert_eq!(state.max_ballot, 7);
    assert_eq!(state.max_accepted.unwrap().command, "hello");
}

#[test]
fn test_rocksdb_command_log_round_trip() {
    let s = temp_rocksdb();
    let mut cl: CommandLog<u32> = CommandLog::new(Box::new(s));
    cl.append(0, &10).unwrap();
    cl.append(1, &20).unwrap();
    cl.set_last_applied(2).unwrap();
    assert_eq!(cl.last_applied().unwrap(), 2);
    let entries = cl.replay().unwrap();
    assert_eq!(entries, vec![(0, 10), (1, 20)]);
}

#[test]
fn test_rocksdb_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut s = RocksDbStorage::open(dir.path()).unwrap();
        s.put(b"acceptor/0/ballot", &serde_json::to_vec(&5u32).unwrap())
            .unwrap();
    }
    {
        let s = RocksDbStorage::open(dir.path()).unwrap();
        let val = s.get(b"acceptor/0/ballot").unwrap().unwrap();
        let ballot: u32 = serde_json::from_slice(&val).unwrap();
        assert_eq!(ballot, 5);
    }
}
