# Claim-Check Latency Benchmark

A benchmark comparing two Kafka payload transports — inline vs claim-check (MongoDB) —
across a multi-topic relay chain, measuring latency (explicitly not a load test).

## Language

**Hop chain**:
The ordered sequence of topics (`topic-01` .. `topic-20`) a message traverses from
entry to terminal. Chain length is a configuration knob.
_Avoid_: pipeline, fan-out

**Stage**:
The processing unit for one topic: consumes from `topic-N`, applies the transform,
republishes to `topic-N+1`. Stage 20 is the terminal stage and republishes nothing.
_Avoid_: step, service, worker

**Hop**:
One stage traversal, timed from consume to Kafka send-ack. Twenty hops make one
chain-total.
_Avoid_: leg, segment

**Twin pair**:
Two messages with identical payload and the same `pairId`, one forced down each
path, so the comparison is same-size, different-transport.
_Avoid_: duplicate, copy

**Path**:
The transport used for the payload: INLINE (payload travels in the Kafka record) or
CLAIM_CHECK (payload lives in MongoDB, record carries a `mongoId`).
_Avoid_: mode, route

**Transform**:
The identical, constant-size, deterministic payload mutation every stage applies on
both paths. Payload size never changes across hops.
_Avoid_: processing, enrichment

**Chain-total latency**:
Wall-clock time from original produce to terminal-stage consumption, computed from
the untouched origin timestamp carried through all hops.
_Avoid_: e2e latency (ambiguous with per-hop)

**Hop trace**:
The per-hop timestamp records appended to a message's envelope at each stage,
enabling a per-message waterfall of its journey.
_Avoid_: audit trail, log

**Claim-check re-check**:
The CLAIM_CHECK stage behavior: fetch document, transform, insert a NEW document,
republish the new `mongoId`. Old documents expire via a 2-minute TTL, never deleted
in the hop path.
_Avoid_: pass-through, reference forwarding
