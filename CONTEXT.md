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

## SRE Agent

A separate, self-improving-workflow context: an agent that watches the benchmark's
own operational health (metrics + logs) and proposes code fixes back into the
producer/consumer/dashboard services.

**Finding**:
A single observed candidate for improvement — either a sustained timing regression
(needs 3 consecutive analysis cycles of confirmation before it graduates to a
proposed fix) or a static code-quality issue (`java-clean-code` rule violation,
surfaces on first detection, no confirmation wait). Findings persist to disk so
they survive an agent restart.
_Avoid_: issue, alert, anomaly (too broad/ambiguous with app-level alerting)

**Proposed fix**:
A finding that has graduated to an actual local git branch + commit with an
explanation and a real diff, shown on the SRE agent's dashboard for approval.
Distinct from a finding still accumulating confirmation cycles.
_Avoid_: suggestion (too vague — a proposed fix always has a concrete diff)

**Analysis cycle**:
One pass of the SRE agent's background loop: scrape current metrics, correlate
with recent logs for the implicated service/stage, optionally call the local LLM.
Runs on an adaptive interval (2 min baseline, doubles on a cycle with zero new
findings, capped at 10 min, resets to 2 min the moment a new finding appears).
_Avoid_: tick, scrape (scrape is reserved for the existing Prometheus-scrape
terminology in dashboard-service)

**Metric-to-class map**:
The lookup the SRE agent uses to go from a metric's name+tags (e.g.
`stage.hop.latency{stage=7,path=CLAIM_CHECK}`) to the specific source file(s)
responsible for that code path, so LLM calls stay targeted instead of scanning
whole service trees.
_Avoid_: routing table (collides with Kafka topic-routing vocabulary already in use)

**sre-agent-service**:
The new, separately-deployed, privileged service hosting this workflow: its own
container (Docker socket + repo mounted in), its own dashboard UI, its own port.
Deliberately kept out of the existing passive, unprivileged `dashboard-service` to
keep the container with host-level Docker access isolated and singular.
_Avoid_: bolting onto dashboard-service, meta-dashboard
