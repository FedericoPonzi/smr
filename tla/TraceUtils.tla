---- MODULE TraceUtils ----
\* Shared helpers for trace validation across all algorithms.
\* Used by TracePaxos, TraceRaft, TraceVSR, etc.

EXTENDS Naturals, Sequences, TLC

\* Check if we've reached the end of the trace
IsEnd(trace, idx) == idx > Len(trace)

\* Get current trace event (1-indexed)
CurrentEvent(trace, idx) == trace[idx]

\* Number of events in the trace
TraceLen(trace) == Len(trace)

====
