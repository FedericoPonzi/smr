#[test]
fn test_smr_multipaxos() {
    let channel = smr::channel::SharedMemoryChannel::new();

    let smr = smr::multipaxos::MultiPaxosNode::new(0, channel);
    smr.run();

    let smr = smr::multipaxos::MultiPaxosNode::new(0, channel);
    smr.run();
}
