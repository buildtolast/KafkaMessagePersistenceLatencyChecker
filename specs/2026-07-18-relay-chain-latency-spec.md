## Problem Statement

The current demo measures inline vs claim-check (MongoDB) transport across a single Kafka hop. That understates the real-world cost difference: in the production use case a message traverses a long pipeline of topics, and at every stage the payload must be read, transformed, and re-persisted. A single-hop comparison cannot answer "what does each transport cost across a 20-stage pipeline, worst case?"

## Solution

Extend the demo into a relay-chain latency benchmark. A twin pair (identical payload, one message per path) enters at `topic-01` and traverses a configurable hop chain of 20 topics. Every stage applies an identical constant-size transform; the CLAIM_CHECK stage performs a full claim-check re-check (Mongo fetch, transform, insert new document, republish new mongoId) while the INLINE stage carries the payload in the Kafka record. The terminal stage records chain-total latency per path. The dashboard visualizes the flow: live heat-mapped stage lanes per path plus a sampled per-message waterfall built from envelope-borne hop traces. This is explicitly a latency measurement, not a load test.

## User Stories

1. As a benchmark operator, I want a twin pair to enter at topic-01 and traverse all 20 topics, so that both transports are measured over the same worst-case pipeline.
2. As a benchmark operator, I want chain length to be a configuration knob, so that I can compare 1-hop, 5-hop, and 20-hop costs without code changes.
3. As a benchmark operator, I want every stage to apply an identical, deterministic, constant-size transform on both paths, so that the measured difference is purely transport and persistence.
4. As a benchmark operator, I want each CLAIM_CHECK stage to fetch, transform, and insert a new Mongo document, so that the benchmark matches my production stage behavior (worst case).
5. As a benchmark operator, I want chain-total latency computed from the untouched origin timestamp, so that the headline number reflects true entry-to-terminal wall-clock time.
6. As a benchmark operator, I want per-hop latency timers (consume to Kafka send-ack, tagged by stage and path), so that I can see where time accumulates along the chain.
7. As a benchmark operator, I want Mongo fetch and insert timers tagged by stage, so that I can attribute claim-check overhead to its persistence segments per hop.
8. As a dashboard viewer, I want the hero comparison to show chain-total p50/p95/p99 per path with an overhead delta badge, so that the inline-vs-claim-check verdict is readable at a glance.
9. As a dashboard viewer, I want two heat-mapped stage lanes (INLINE above, CLAIM_CHECK below) with per-stage rolling average hop latency, so that I can watch the flow and spot hot stages live.
10. As a dashboard viewer, I want a sampled per-message waterfall showing one pair's journey hop by hop, side by side per path, so that I can see one concrete message flow from topic-01 to topic-20.
11. As a dashboard viewer, I want the per-hop detail aggregated (average hop cost by segment) rather than 40 raw percentile lines, so that the chart stays readable.
12. As a benchmark operator, I want the default publish rate slowed to 0.5 pairs/s, so that latency is measured on a loaded-but-unsaturated system rather than a collapsing one.
13. As a benchmark operator, I want old claim-check documents reaped by a 2-minute TTL instead of deletes in the hop path, so that storage stays bounded without polluting hop timings.
14. As a benchmark operator, I want all 20 stage listeners hosted in the existing consumer-service image (3 replicas), so that the benchmark runs on one laptop without 20 JVMs distorting the numbers.
15. As a benchmark operator, I want the pair generator to send both twins to topic-01 (replacing the single messages topic), so that the existing forced-path pairing mechanism drives the chain.
16. As a benchmark operator, I want hop traces appended to the envelope at every stage, so that the terminal consumer has a complete per-message journey with no new metrics infrastructure.
17. As a benchmark operator, I want a missing claim-check document at any stage to be logged and skipped without retry storms, so that a single failure does not distort the run.
18. As a benchmark operator, I want the existing resilience story preserved (kill one broker or one Mongo node and flow continues), so that chain runs survive node loss.
19. As a developer, I want the stage behavior tested at a single pure seam with no Kafka or Mongo, so that chain logic is verified fast and deterministically.
20. As a developer, I want an integration test running a short chain end to end per path, so that wiring across topics, Mongo, and metrics is proven before a full 20-topic run.
21. As a benchmark operator, I want a README procedure for reading the results and finding the saturation point separately, so that latency and load exploration stay distinct experiments.

## Implementation Decisions

- Topology is a relay chain: stage N consumes `topic-N`, republishes to `topic-N+1`; stage 20 is terminal. Topics `topic-01`..`topic-20`, 3 partitions, RF 3, replacing the single `messages` topic. Chain length is configuration (`app.chain.length`, default 20).
- Stage behavior (worst case, per glossary "claim-check re-check"): CLAIM_CHECK stage does Mongo fetch → transform → insert NEW document → republish new mongoId. INLINE stage does deserialize → same transform → republish inline. The transform is deterministic and constant-size; payload size never changes across hops.
- The one new seam is a pure StageProcessor in consumer-service: (envelope, stage config, clock) → StageResult describing what to store, what to publish next, and what to measure. It subsumes the existing ClaimCheckResolver for stage work and reuses the existing PayloadReader/PayloadStore ports; a stage needs both read and write access to Mongo.
- Envelope changes: the origin produce timestamp is carried untouched through all hops for chain-total latency; a hop-trace array (stage, timestamps) is appended at each stage. Existing pairId/forcedPath semantics unchanged.
- Metrics: chain-total latency timer per path recorded by the terminal stage; hop latency timer (consume to send-ack) tagged stage and path; Mongo fetch/insert timers gain a stage tag. Existing counters keep working.
- Dashboard: hero strip switches to chain-total percentiles per path with the overhead delta badge; new flow visualization with two heat-mapped stage lanes fed by the existing 2 s scrape/SSE pipeline; new sampled waterfall view fed by hop traces surfaced by the terminal consumer (latest sampled trace exposed to the dashboard); per-hop data is aggregated, not plotted as 40 series.
- Storage: 2-minute TTL index on the payload collection; no deletes in the hop path. Payloads are disposable; latency numbers are the product.
- Deployment: single consumer-service image hosts all stage listeners via a config-driven list; 3 replicas as today; Kafka group protocol spreads partitions across replicas.
- Default load: 0.5 pairs/s (config knob retained). Rationale: 5 pairs/s at 20 hops would be ~200 MB/s produce traffic tripled by RF 3 — saturation, not latency measurement.
- Missing-document handling at any stage: log error, skip, no retry.
- Domain vocabulary per CONTEXT.md: hop chain, stage, hop, twin pair, path, transform, chain-total latency, hop trace, claim-check re-check.

## Testing Decisions

- Good tests exercise external behavior at the seam: envelope in → StageResult out. No assertions on internals, no Kafka/Mongo in unit tests (Mongo behind the existing PayloadReader/PayloadStore fakes).
- StageProcessor unit tests: INLINE hop (transform applied, republish to next topic, trace appended), CLAIM_CHECK hop (fetch → transform → new insert → new mongoId republished), terminal stage (no republish, chain-total measured from origin timestamp), missing document (skip, no retry), constant payload size across hops, chain-length boundary (stage N vs terminal).
- Producer tests extended minimally: pair generator targets topic-01.
- Dashboard MetricsAggregator tests extended for stage-tagged samples and the waterfall model, following the existing MetricsAggregatorTest style.
- Integration: existing Testcontainers pattern (single Kafka + single Mongo) extended to a short chain (length 3), one end-to-end assertion per path on chain-total latency presence and hop-trace completeness. Prior art: ConsumerIntegrationTest, ProducerIntegrationTest.
- Verify gate remains `./gradlew test` per service; coverage target 80% on chain logic.

## Out of Scope

- Load testing / saturation-point automation (manual procedure only).
- Payload sizes that change per stage; payload cleanup beyond the TTL.
- Per-stage containers or independent stage microservices.
- Animated real-time per-message tracing infrastructure (the waterfall uses sampled envelope-borne traces only).
- Auth/TLS, schema registry, Kafka Streams, sharded Mongo, third-party observability stacks (unchanged from rev 2).

## Further Notes

- Supersedes the single-hop comparison workload in specs/2026-07-15-kafka-claim-check-design.md (rev 2); that spec's cluster topology, envelope pairing, metrics plumbing, and dashboard skeleton are retained.
- With hops serialized, one pair spans roughly 1–3 s in flight across 20 stages; at 0.5 pairs/s several messages are in the pipeline simultaneously, which is sufficient signal for the percentile charts.
- Implementation routing follows CLAUDE.md §12 (local-LLM codegen loop with review), unchanged.
