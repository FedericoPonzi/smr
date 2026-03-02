//! Paxos-specific trace events for TLA+ conformance checking.
//!
//! Emits structured `tracing::trace!()` events at protocol linearization points
//! using the `"paxos_trace"` target. These events are zero-cost when no
//! tracing subscriber is attached.
//!
//! To collect traces, enable TRACE level for this target:
//!   RUST_LOG=paxos_trace=trace cargo run ...
//!
//! Or programmatically with a JSON tracing-subscriber layer in tests.

/// Phase1a: proposer sends Prepare.
pub(crate) fn trace_phase1a(node_id: u32, instance_id: u64, ballot: u32) {
    tracing::trace!(
        target: "paxos_trace",
        action = "Phase1a",
        node_id,
        instance_id,
        ballot,
        sender = node_id,
    );
}

/// Phase1b: acceptor responds with Promise (updates maxBal).
pub(crate) fn trace_phase1b(
    node_id: u32,
    instance_id: u64,
    ballot: u32,
    sender: u32,
    max_bal: u32,
    max_v_bal: Option<u32>,
    max_val: Option<&str>,
) {
    tracing::trace!(
        target: "paxos_trace",
        action = "Phase1b",
        node_id,
        instance_id,
        ballot,
        sender,
        max_bal,
        max_v_bal = max_v_bal.unwrap_or(0),
        max_val = max_val.unwrap_or(""),
    );
}

/// NackPrepare: acceptor rejects Prepare.
pub(crate) fn trace_nack_prepare(
    node_id: u32,
    instance_id: u64,
    ballot: u32,
    sender: u32,
    max_bal: u32,
) {
    tracing::trace!(
        target: "paxos_trace",
        action = "NackPrepare",
        node_id,
        instance_id,
        ballot,
        sender,
        max_bal,
    );
}

/// Phase2a: proposer sends Accept.
pub(crate) fn trace_phase2a(node_id: u32, instance_id: u64, ballot: u32, value: &str) {
    tracing::trace!(
        target: "paxos_trace",
        action = "Phase2a",
        node_id,
        instance_id,
        ballot,
        sender = node_id,
        max_val = value,
    );
}

/// Phase2b: acceptor accepts value (updates maxVBal, maxVal).
pub(crate) fn trace_phase2b(
    node_id: u32,
    instance_id: u64,
    ballot: u32,
    sender: u32,
    max_bal: u32,
    max_v_bal: u32,
    max_val: &str,
) {
    tracing::trace!(
        target: "paxos_trace",
        action = "Phase2b",
        node_id,
        instance_id,
        ballot,
        sender,
        max_bal,
        max_v_bal,
        max_val,
    );
}

/// NackAccept: acceptor rejects Accept.
pub(crate) fn trace_nack_accept(
    node_id: u32,
    instance_id: u64,
    ballot: u32,
    sender: u32,
    max_bal: u32,
) {
    tracing::trace!(
        target: "paxos_trace",
        action = "NackAccept",
        node_id,
        instance_id,
        ballot,
        sender,
        max_bal,
    );
}

/// Learn: learner has reached quorum.
pub(crate) fn trace_learn(node_id: u32, instance_id: u64, ballot: u32, value: &str) {
    tracing::trace!(
        target: "paxos_trace",
        action = "Learn",
        node_id,
        instance_id,
        ballot,
        sender = node_id,
        max_val = value,
    );
}
