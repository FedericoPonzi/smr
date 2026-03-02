---- MODULE Paxos ----
\* Single-decree Paxos (Synod) specification.
\* This is the "oracle" against which implementation traces are validated.
\*
\* Based on Lamport's "Paxos Made Simple" and the Voting spec from
\* the TLA+ examples repository.

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Nodes,      \* Set of node IDs (e.g., {0, 1, 2})
    Values,     \* Set of possible values
    Quorum      \* Quorum size (majority)

VARIABLES
    maxBal,     \* maxBal[a]: highest ballot acceptor a has promised
    maxVBal,    \* maxVBal[a]: ballot of highest accepted proposal by a
    maxVal,     \* maxVal[a]: value of highest accepted proposal by a
    msgs        \* Set of all messages sent

vars == <<maxBal, maxVBal, maxVal, msgs>>

\* Message types
Send(m) == msgs' = msgs \union {m}

\* Phase 1a: proposer p sends Prepare with ballot b
Phase1a(p, b) ==
    /\ Send([type |-> "Phase1a", sender |-> p, ballot |-> b])
    /\ UNCHANGED <<maxBal, maxVBal, maxVal>>

\* Phase 1b: acceptor a responds to Prepare
\* Acceptor promises not to accept any ballot < b
Phase1b(a) ==
    \E m \in msgs :
        /\ m.type = "Phase1a"
        /\ m.ballot > maxBal[a]
        /\ maxBal' = [maxBal EXCEPT ![a] = m.ballot]
        /\ Send([type     |-> "Phase1b",
                 sender   |-> a,
                 ballot   |-> m.ballot,
                 maxVBal  |-> maxVBal[a],
                 maxVal   |-> maxVal[a]])
        /\ UNCHANGED <<maxVBal, maxVal>>

\* Phase 2a: proposer p sends Accept for ballot b with value v
\* The proposer must have received Phase1b from a quorum,
\* and v is either the value from the highest-ballot Phase1b
\* or any value if no acceptor had accepted anything.
Phase2a(p, b, v) ==
    /\ ~\E m \in msgs : m.type = "Phase2a" /\ m.ballot = b
    /\ \E Q \in SUBSET {m \in msgs : m.type = "Phase1b" /\ m.ballot = b} :
        /\ Cardinality({m.sender : m \in Q}) >= Quorum
        /\ \/ \A m \in Q : m.maxVBal = -1  \* No one accepted anything
              /\ v \in Values
           \/ \E m \in Q :
                /\ m.maxVBal >= 0
                /\ m.maxVal = v
                /\ \A m2 \in Q : m2.maxVBal =< m.maxVBal
    /\ Send([type |-> "Phase2a", sender |-> p, ballot |-> b, value |-> v])
    /\ UNCHANGED <<maxBal, maxVBal, maxVal>>

\* Phase 2b: acceptor a accepts proposal with ballot b and value v
Phase2b(a) ==
    \E m \in msgs :
        /\ m.type = "Phase2a"
        /\ m.ballot >= maxBal[a]
        /\ maxBal'  = [maxBal  EXCEPT ![a] = m.ballot]
        /\ maxVBal' = [maxVBal EXCEPT ![a] = m.ballot]
        /\ maxVal'  = [maxVal  EXCEPT ![a] = m.value]
        /\ Send([type |-> "Phase2b", sender |-> a, ballot |-> m.ballot, value |-> m.value])

\* A value v is chosen if a quorum of acceptors accepted it at the same ballot
Chosen(v) ==
    \E b \in {m.ballot : m \in msgs} :
        \E Q \in SUBSET {m \in msgs : m.type = "Phase2b" /\ m.ballot = b /\ m.value = v} :
            Cardinality({m.sender : m \in Q}) >= Quorum

\* --- Spec ---

Init ==
    /\ maxBal  = [a \in Nodes |-> -1]
    /\ maxVBal = [a \in Nodes |-> -1]
    /\ maxVal  = [a \in Nodes |-> "None"]
    /\ msgs    = {}

Next ==
    \/ \E p \in Nodes, b \in Nat : Phase1a(p, b)
    \/ \E a \in Nodes : Phase1b(a)
    \/ \E p \in Nodes, b \in Nat, v \in Values : Phase2a(p, b, v)
    \/ \E a \in Nodes : Phase2b(a)

Spec == Init /\ [][Next]_vars

\* --- Safety Invariants ---

\* Agreement: at most one value can be chosen
Agreement ==
    \A v1, v2 \in Values :
        Chosen(v1) /\ Chosen(v2) => v1 = v2

\* Ballot monotonicity: maxBal never decreases
BallotMonotonicity ==
    \A a \in Nodes : maxBal'[a] >= maxBal[a]

====
