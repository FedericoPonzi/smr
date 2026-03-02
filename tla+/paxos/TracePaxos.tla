---- MODULE TracePaxos ----
\* Trace validation spec for Paxos.
\*
\* Loads an NDJSON trace collected from the Rust implementation and
\* replays it step-by-step, checking that each step is a valid
\* transition of the Paxos specification.
\*
\* Usage:
\*   java -Dtlc2.tool.queue.IStateQueue=StateDeque \
\*     -jar tla2tools.jar -config TracePaxos.cfg TracePaxos.tla

EXTENDS Paxos, TraceData, TLC, Sequences, Integers, FiniteSets

\* The trace loaded from NDJSON, as a sequence of records.
\* Each record has fields: action, node_id, ballot, sender, instance_id,
\* max_bal, max_v_bal, max_val
CONSTANT Trace

VARIABLE traceIdx

traceVars == <<vars, traceIdx>>

\* Map trace action names to spec actions
TraceNext ==
    /\ traceIdx <= Len(Trace)
    /\ LET evt == Trace[traceIdx] IN
       /\ traceIdx' = traceIdx + 1
       /\ CASE evt.action = "Phase1a" ->
                /\ Phase1a(evt.node_id, evt.ballot)

            [] evt.action = "Phase1b" ->
                /\ Phase1b(evt.node_id)
                \* Constrain: after this step, acceptor state matches trace
                /\ maxBal'[evt.node_id] = evt.ballot

            [] evt.action = "NackPrepare" ->
                \* Nack: acceptor rejects, state doesn't change
                /\ UNCHANGED vars

            [] evt.action = "Phase2a" ->
                /\ \E v \in Values : Phase2a(evt.node_id, evt.ballot, v)

            [] evt.action = "Phase2b" ->
                /\ Phase2b(evt.node_id)
                /\ maxBal'[evt.node_id]  = evt.ballot
                /\ maxVBal'[evt.node_id] = evt.ballot

            [] evt.action = "NackAccept" ->
                \* Nack: acceptor rejects, state doesn't change
                /\ UNCHANGED vars

            [] evt.action = "Learn" ->
                \* Learn is an observation, not a state change in the spec
                /\ UNCHANGED vars

            [] OTHER -> UNCHANGED vars

\* When trace is exhausted, allow stuttering
TraceDone ==
    /\ traceIdx > Len(Trace)
    /\ UNCHANGED traceVars

TraceInit ==
    /\ Init
    /\ traceIdx = 1

TraceSpec == TraceInit /\ [][TraceNext \/ TraceDone]_traceVars

\* Check agreement as an invariant
TraceAgreement == Agreement

====
