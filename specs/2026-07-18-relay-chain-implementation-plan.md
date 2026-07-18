# Implementation Plan: 20-Topic Relay-Chain Latency Benchmark

Source: specs/2026-07-18-relay-chain-latency-spec.md (GitHub issue #1, ready-for-agent)
Supersedes: specs/2026-07-15-kafka-claim-check-design.md (rev 2, single-hop)

## Codegen routing

Local LLM for this project is llama.cpp at `http://127.0.0.1:8080` (health OK,
serving `unsloth/gemma-4-31B-it-GGUF:UD-Q4_K_XL`) — NOT the `:8888` Unsloth
Studio wrapper, whose agentic middleware hangs plain completions (see project
memory unsloth-studio-middleware-hijack). Route codegen through
`tools/llm-gen.sh` / `tools/llm-loop.py` with
`UNSLOTH_BASE_URL=http://127.0.0.1:8080 UNSLOTH_API_KEY=local
UNSLOTH_MODEL='unsloth/gemma-4-31B-it-GGUF:UD-Q4_K_XL' LLM_NO_THINK=1` (it's a
reasoning model; without LLM_NO_THINK large specs exhaust max_tokens on
reasoning_content). Fable review of generated output before acceptance;
token-report.py after each step.

## Plan review (Fable, adversarial, 5 rounds) — defects folded in below

**Round 5** verdict: ready to implement, one trivial two-line addendum: (19)
consumer-service's `application.yml` already sets
`management.metrics.distribution.percentiles-histogram` = true for
`consumer.processing` and `consumer.mongo.fetch` — round-3 fix (12)'s
"count/sum only, no percentiles" for stage-tagged timers requires removing
those `percentiles-histogram` entries (and `consumer.processing`'s entry,
alongside removing `consumer.e2e.latency` per round-3 fix 13), or the
histogram cardinality blowup fix (12) was meant to prevent still happens via
yml config rather than code. `chain.e2e.latency`'s percentiles must use
`distribution.percentiles` (client-computed summary quantiles), not
`percentiles-histogram` (per project memory: histogram-type timers silently
drop `publishPercentiles`). All other checks (TTL literal-edit safety,
consumer-service auto-wiring via default component scan, `MessageEnvelope`
confirmed as an immutable record on both sides making "new instance per hop"
the only possible implementation) came back clean — no further defects
found.

**Round 4** verdict: needs revision (narrow), verified against actual repo
files (docker-compose.yml, consumer-service/build.gradle,
producer-service/application.yml): (14) round-3's TTL fix was operationalized
backwards — mongo1-3 declare no named volumes, so every `compose up` is a
**fresh** volume by default (the normal case), meaning collMod on a
nonexistent collection fails; the correct default-path fix is simply editing
the existing `createIndex(...,{expireAfterSeconds: 1800})` call to `120`
directly, with a guarded collMod/dropIndex only as a documented fallback for
someone reusing an old volume. Also the plan's collMod syntax was invalid
mongosh (needs `db.runCommand({collMod: ..., index: {...}})` form, not a
bare method call). (15) `spring-boot-starter-web` confirmed present on
consumer-service's classpath — no new dependency needed; also, the trace
endpoint must be served on port 8091 (the existing consumer health/actuator
port) so the dashboard's per-instance fan-out hits the same host:port already
used for Prometheus scraping. (16) `acks=all` confirmed present in
producer-service's `application.yml` — the round-3 fix's premise holds;
worth also mirroring `enable.idempotence` if producer-service sets it. (17)
real seam-shape gap: `StageResult.Terminal(chainTotalLatency)` as typed can't
carry the completed hop-trace the waterfall needs — it must be
`Terminal(chainTotalLatencyNanos, finalEnvelope)`, and the plan must say
which component writes it into a per-replica in-memory trace buffer (bounded,
keyed by pairId/path) backing `/api/traces/latest`. (18) minor: since each
terminated message lands on exactly one of 3 replicas, the dashboard's three
trace responses are disjoint message sets — state the merge rule explicitly
(newest-N across replicas by terminal consumedAt, matched by pairId as
already specified) so the UI doesn't flicker between unrelated replicas'
messages.

**Round 3** verdict: needs revision, verified against actual code
(EnvelopeListener, ClaimCheckResolver, ConsumerMetrics, MongoPayloadStore,
ScrapeScheduler, PrometheusScraper, docker-compose.yml): round-2 fixes 5-8
confirmed coherent. Found: (9) a TTL index already exists on `createdAt` at
`expireAfterSeconds: 1800` — the plan's "add a 2-minute TTL" must become
"change" (via `collMod`, since Mongo rejects re-creating an index with a
different `expireAfterSeconds`), with a note on the volume-reuse case; (10)
consumer-service has no Kafka producer today (no KafkaTemplate anywhere) —
republishing is an unlisted code addition, and it must use `acks=all` to
match the RF3/min.insync=2 topics and stay comparable to the producer's
send-ack semantics; (11) the trace fan-out is a real second scrape path,
not reuse of the existing one — needs its own REST controller (verify
spring-web is on consumer-service's classpath), its own per-target HTTP call
inside the scrape loop with timeout budget considered, its own JSON model,
a `ComparisonModel` signature change, explicit exclusion of producer-service
from trace-target fan-out, and an explicit statement that the terminal stage
still appends a `HopRecord` (consumedAt set, publishedAt absent) so the
waterfall's last lane isn't missing; (12) per-stage timers should publish
count/sum only, no percentiles (percentiles stay on `chain.e2e.latency`
only) to avoid a wasted ~240 series/replica of unused quantiles; (13) minor:
state the fate of the existing `consumer.e2e.latency` timer during
migration.

**Round 1** verdict: needs revision, 4 points: (1) listener wiring hand-waved,
(2) fairness invariant under hop-trace growth not stated precisely, (3)
terminal-trace waterfall design didn't account for 3 replicas, (4) suspected
`producedAtNanos` clock-domain bug.

**Round 2** verdict: needs revision (narrowly), verified against the actual
code: fix (4)'s premise was wrong — `ClaimCheckRouter`/`MessageEnvelope`
already derive `producedAtEpochNanos` from `Instant.now()` (wall clock), not
`System.nanoTime()`; no bug existed there, only a naming/consistency note
remains. Found instead: (5) terminal condition must be `stage >= chain.length`
not `==`, to handle leftover topics from a prior run when chain.length shrinks;
(6) chain-total measurement point and a wasted terminal-stage Mongo insert
were unspecified; (7) `PayloadStore` exists only in producer-service —
consumer-service needs its own port+impl, and both must write `createdAt`
identically or the TTL breaks; (8) minor: anchor the topic-pattern regex,
seed topics before consumer startup in the chain-3 integration test.

## Bootstrap (done directly, not delegated)

1. docker-compose.yml: replace single `messages` topic with topic-init for
   `topic-01`..`topic-20` (3 partitions, RF 3, min.insync.replicas=2 each).
   Keep existing kafka1-3 / mongo1-3 / producer / consumer(x3) / dashboard
   services and healthchecks unchanged.
2. Add `app.chain.length` (default 20) and `app.load.pairs-per-second`
   (default 0.5) properties to producer-service and consumer-service
   application.yml.
3. **Change the existing TTL index's value (round-4 corrected fix):**
   `docker-compose.yml`'s mongo-init (an ephemeral `mongo:7` container
   running `mongosh --eval`) already creates `{createdAt: 1}` with
   `expireAfterSeconds: 1800`. Mongo containers here declare no named
   volumes, so **every `compose up` starts fresh** — the default-path fix is
   simply editing the existing `createIndex(...)` call's argument from
   `1800` to `120`, no collMod needed for the common case. For the edge case
   of someone reusing an old volume where the collection/index already
   exists at 1800s, guard with
   `db.getSiblingDB("claimcheck").runCommand({collMod: "large_payloads",
   index: {keyPattern: {createdAt: 1}, expireAfterSeconds: 120}})` as a
   fallback (note: this is the correct mongosh form — a bare `collMod
   large_payloads, index: {...}` method call is invalid syntax).

Verify: `docker compose config` validates; existing `./gradlew test` per
service still green before any new code lands (regression baseline).

## Core seam (new): StageProcessor (consumer-service)

Pure function, no Kafka/Mongo:
`StageProcessor.process(MessageEnvelope envelope, StageConfig config, Clock clock) -> StageResult`

- `StageConfig`: stage number, next topic name (or null if terminal),
  path-agnostic (the envelope's path field drives behavior).
- `StageResult`: sealed outcome — `Republish(envelope, targetTopic)` or
  `Terminal(chainTotalLatencyNanos, finalEnvelope)` or `Skipped(reason)`
  (missing document case). **Round-4 fix:** `Terminal` must carry the
  final envelope (with its completed hop-trace), not just the latency
  number — the dashboard's waterfall needs the full trace, and dropping it
  at the seam boundary makes it unrecoverable downstream.
- INLINE behavior: deserialize, apply transform, append hop-trace entry,
  return Republish (or Terminal if config says stage 20).
- CLAIM_CHECK behavior: read via existing `PayloadReader` port, transform,
  write via a `PayloadStore` port (new document, new id), envelope carries
  new mongoId, append hop-trace entry, return Republish/Terminal.
- **Terminal stage (round 2 fix):** terminal condition is `stage >=
  chain.length`, not `==` — handles leftover higher-numbered topics from a
  prior run when `chain.length` is lowered (they still exist in the compose
  volume and the pattern listener still matches them). At the terminal
  stage, CLAIM_CHECK does NOT perform the insert — there is no next hop to
  read it, so it would be a dead write and a systematic bias in the
  CLAIM_CHECK headline latency. Chain-total latency is measured as
  `(this stage's consume timestamp) - producedAtEpochNanos`, taken **before**
  any stage work runs, symmetric for both paths.
- Transform: deterministic, constant-size (e.g. overwrite a fixed-size
  header/checksum region) — same function both paths. Not applied at the
  terminal stage (nothing downstream needs it).
- Missing document (claim-check fetch returns empty): log, return Skipped,
  no retry.
- Subsumes existing `ClaimCheckResolver` for stage work; existing
  `PayloadReader` interface reused unmodified.
- **New port needed (round 2 fix):** `PayloadStore` currently exists only in
  producer-service. Consumer-service needs its own `PayloadStore` port +
  Mongo implementation (mirroring the producer's, no shared module — same
  pattern as `MessageEnvelope` duplication). Both the producer's insert and
  the consumer's hop-insert MUST write the `createdAt` field identically
  (same clock derivation), since the 2-minute TTL index is keyed on
  `createdAt` — any divergence means hop-inserted documents either never
  expire (unbounded Mongo growth) or expire at the wrong time.

Envelope changes (consumer + producer `MessageEnvelope`, kept in sync as
today — no shared module):
- **Clock domain (corrected, round 2):** `producedAtEpochNanos` is already
  wall-clock (`Instant.now()`-derived) in the current code — no
  System.nanoTime() bug exists. No rename needed; keep the field as-is and
  reuse it as the origin timestamp for chain-total. All new hop-trace
  timestamps must use the same wall-clock derivation for consistency
  (single-Docker-host clock skew is negligible for this demo; note it in
  the README, no code mitigation needed).
- Add hop-trace list: `List<HopRecord>` where `HopRecord{stage,
  consumedAtEpochNanos, publishedAtEpochNanos}` (same wall-clock domain as
  `producedAtEpochNanos`), appended (new list) not mutated, on every hop.
- `mongoId` field updated to the newly-inserted id on CLAIM_CHECK stages.
- **Fairness invariant, stated precisely (defect 2):** hop-trace entries live
  only in the Kafka envelope, on both paths identically — never written into
  the Mongo document. The Mongo document stores payload only (constant size
  every hop). So per-hop record size = envelope (identical on both paths,
  including trace) + payload-or-mongoId. The trace's growth is symmetric and
  does not skew the comparison; the only measured difference is inline
  payload bytes vs a mongoId string, which is the intended fairness point.

## Wiring (consumer-service) — listener design decided (defect 1)

- **Decision:** a single `@KafkaListener` bound to a topic pattern
  (`topic-\d{2}`, anchored to exactly two digits per round-2 minor finding —
  avoids incidental matches like a hypothetical `topic-0` or `topic-100`;
  one consumer group, one container factory) rather than 20
  discrete listener methods or runtime-registered containers. The stage
  number is derived per-message from `@Header(KafkaHeaders.RECEIVED_TOPIC)`
  (parse the trailing digits), not from static per-listener config — this is
  what makes "config-driven chain length" actually work without touching
  listener wiring when `app.chain.length` changes (topics 21+ simply aren't
  created by compose, so the pattern subscribes to whatever exists).
- Each invocation builds `StageConfig` (stage number, next topic = stage+1
  or null if stage == chain.length) from the parsed header + the injected
  chain-length property, then delegates to `StageProcessor`.
- Concurrency/assignment: explicitly configure `CooperativeStickyAssignor`
  (not the default `RangeAssignor`) given 20 topics × 3 partitions = 60
  partitions across 3 replicas, to avoid assignment skew.
- Startup ordering (corrected, round 2): `topicPattern` subscription DOES
  discover newly-created matching topics at runtime via periodic metadata
  refresh (default `metadata.max.age.ms` = 5 min) — it is not a one-shot
  snapshot. The topic-init container gating consumer-service startup is
  still the right call for determinism (no waiting on a 5-minute refresh
  window at demo start), via existing
  `depends_on: condition: service_completed_successfully`.
- Reuse existing 3-replica deployment; the pattern-subscribed group spreads
  all topic-partitions across replicas — no new deployment unit.
- **New producer capability needed in consumer-service (round-3 fix):**
  consumer-service currently has no Kafka producer at all (no
  `KafkaTemplate`, no producer config) — republishing to `topic-N+1` is a
  net-new code addition, not "reuse the existing Kafka template" (that only
  exists in producer-service). Configure it with `acks=all` to match
  producer-service's semantics and the topics' `min.insync.replicas=2`;
  otherwise hop latencies would undercount replication wait relative to the
  entry hop and the comparison stops being apples-to-apples. Confirmed
  (round-4): producer-service's `application.yml` already sets
  `spring.kafka.producer.acks: all`; also check whether it sets
  `enable.idempotence` and mirror that setting in the new consumer-service
  producer config for consistency.

## Wiring (producer-service)

- `PairGenerator`/`ClaimCheckRouter`: change target topic from `messages` to
  `topic-01`. Rate default changes to 0.5 (config, not hardcoded). No other
  routing-logic change — this is stage 0 (entry), not part of the StageProcessor
  chain.

## Metrics (Micrometer, tag additions)

- New: `chain.e2e.latency` (Timer, tag `path`) recorded only at terminal stage,
  from origin timestamp. **This is the only timer that publishes
  percentiles** (round-3 fix) — it's the headline metric the dashboard's
  hero strip needs p50/p95/p99 for.
- New: `stage.hop.latency` (Timer, tags `stage`, `path`) — consume to
  send-ack, recorded at every non-terminal stage. **Percentiles disabled,
  count/sum only** (round-3 fix): the heat-mapped lanes only ever display
  averages, so 20 stages × 2 paths of quantile histograms would be ~240
  unused series/replica.
- Existing `consumer.mongo.fetch` / producer-side insert timers gain a
  `stage` tag (claim-check stages only); same round-3 fix applies —
  count/sum only, no percentiles, once stage-tagged.
- Existing `consumer.messages`/`consumer.bytes` counters keep working
  per-stage (tag `stage`).
- **Fate of `consumer.e2e.latency` (round-3 minor fix):** this existing
  single-hop timer is superseded by `chain.e2e.latency`; remove it rather
  than keep two overlapping "total latency" timers, and update the
  dashboard's hero strip to read the new name.
- **Yml-level percentile config (round-5 fix):** consumer-service's
  `application.yml` sets `management.metrics.distribution.percentiles-
  histogram.consumer.processing` and `...consumer.mongo.fetch` to `true`.
  Remove `consumer.processing`'s entry (timer is being removed) and remove
  `consumer.mongo.fetch`'s `percentiles-histogram` entry once it becomes
  stage-tagged (histograms would otherwise defeat the code-level "count/sum
  only" intent). Configure `chain.e2e.latency`'s percentiles via
  `distribution.percentiles` (client-computed summary quantiles), not
  `percentiles-histogram` — histogram-type timers silently drop
  `publishPercentiles` in this Micrometer/Prometheus setup.

## Dashboard (dashboard-service)

- `MetricsAggregator`: extend comparison model with (a) chain-total
  percentiles per path (replaces single-hop e2e as the hero number), (b)
  per-stage average hop latency per path (for the heat-mapped lanes — average
  only, not raw series, to avoid 40-line charts).
- **Waterfall/trace design fixed for 3 replicas (defect 3), and specified as
  real code (round-3 fix):** each replica only sees traces for the topic-20
  partitions it owns, so a single replica's "last trace" is not
  representative. Design: each terminal-stage replica exposes a small
  `/api/traces/latest` endpoint returning its most recently completed
  `{pairId, path, hopTrace}` per path it has seen. This is a **second,
  separate scrape path**, not reuse of the existing Prometheus scrape —
  concretely:
  - Add a new REST controller to consumer-service (its first non-actuator
    HTTP endpoint; confirmed round-4: `spring-boot-starter-web` is already
    on consumer-service's classpath, no new dependency needed). Serve it on
    port 8091, the same port already used for the consumer's actuator
    health/Prometheus endpoints and referenced in `DASHBOARD_TARGETS`.
  - **Trace buffer ownership (round-4 fix):** the listener wiring, on
    receiving `StageResult.Terminal(latency, finalEnvelope)` from
    `StageProcessor`, writes `{pairId, path, hopTrace}` from `finalEnvelope`
    into a small in-memory, per-replica, bounded buffer (e.g. last N per
    path) keyed by pairId; the new REST controller reads from that buffer.
  - `ScrapeScheduler`/`PrometheusScraper` are hard-wired to one
    `/actuator/prometheus` GET per target parsed as Prometheus text; add a
    second per-consumer-target JSON GET to `/api/traces/latest` inside (or
    alongside) the existing fixed-rate scrape loop, with its own timeout
    budget accounted for (today's loop is sequential across targets within
    a 2s tick — 3 extra calls must not blow that budget; consider
    parallelizing or a longer/independent tick for trace fetches).
  - New JSON DTO + Jackson parsing, distinct from the Prometheus text
    parser; `ComparisonModel`/`MetricsAggregator.aggregate()` signature
    changes to carry the matched-pair trace alongside the existing
    percentile/throughput fields.
  - **Explicitly exclude producer-service** from trace-endpoint fan-out
    (`app.dashboard.targets` today includes it for Prometheus scraping; it
    has no trace endpoint).
  - **Terminal HopRecord completeness:** the terminal stage still appends a
    `HopRecord` to the trace (consumedAt set, publishedAt absent/null) even
    though it doesn't republish — otherwise the waterfall's final lane is
    missing and the trace looks truncated one hop short.
  - Dashboard picks the overall-newest INLINE trace and CLAIM_CHECK trace
    **matched by pairId** so the waterfall shows the two twins of the same
    logical pair, not two unrelated messages from different
    replicas/moments. If the matching twin hasn't arrived yet (still
    mid-chain), the aggregator holds the most recent complete matched pair
    rather than showing a lone twin.
  - **Cross-replica merge rule, stated explicitly (round-4 fix):** since
    each terminated message lands on exactly one of the 3 consumer
    replicas, the three `/api/traces/latest` responses are disjoint message
    sets, not overlapping views. The aggregator merges by taking, across
    all 3 replicas' responses, the newest-by-terminal-consumedAt trace per
    path, then applies the pairId-matching rule above — otherwise the UI
    flickers between unrelated messages from different replicas.
- UI (`app.js`/`index.html`): hero strip switches to chain-total; add two
  heat-mapped stage-lane rows (INLINE/CLAIM_CHECK) with per-stage color/label
  from aggregator; add a waterfall panel rendering the latest sampled trace,
  both paths side by side. No new JS libraries — vanilla + existing Chart.js.

## Testing

1. **StageProcessor unit tests (TDD, RED first, via Sonnet subagent):**
   INLINE hop transform+republish+trace-append; CLAIM_CHECK hop fetch→
   transform→new-insert→republish; terminal stage (no republish, chain-total
   computed); missing-document skip/no-retry; constant payload size
   assertion; stage-N-vs-terminal boundary. Fakes for PayloadReader/
   PayloadStore (existing test doubles pattern from ClaimCheckResolverTest).
2. **Producer test update:** PairGenerator/router target topic-01 (minimal
   diff to existing ClaimCheckRouterTest/ProducerIntegrationTest).
3. **Dashboard tests:** extend MetricsAggregatorTest for stage-tagged
   samples and the waterfall/trace model, following existing style.
4. **Integration:** extend existing Testcontainers pattern to a short chain
   (length 3 via config override) — one end-to-end assertion per path on
   chain-total latency presence and hop-trace completeness (mirrors
   ConsumerIntegrationTest/ProducerIntegrationTest). Round-2 minor fix:
   create `topic-01..03` before the consumer context starts (mirror compose's
   `depends_on: service_completed_successfully` ordering) so the
   pattern-subscribed listener's first assignment doesn't race a metadata
   refresh and flake.
5. Verify gate unchanged: `./gradlew test` per service, 80% coverage target
   on chain logic (StageProcessor + aggregator additions).

## Sequencing (to keep every intermediate state green)

1. Bootstrap: compose topics + TTL + config knobs → verify compose config,
   baseline tests still pass.
2. Envelope changes (origin timestamp preserved, hop-trace field) in both
   producer and consumer `MessageEnvelope` — round-trip serialization test
   first.
3. StageProcessor: tests RED → implementation GREEN (Sonnet subagent per
   fallback routing).
4. EnvelopeListener generalization + producer topic-01 retarget — wire
   StageProcessor in; short-chain integration test (length 3) GREEN.
5. Metrics additions (chain-total, hop timers, stage tags) — unit-tested
   alongside StageProcessor; confirm `/actuator/prometheus` exposes new series.
6. Dashboard aggregator + waterfall model — tests first, then UI panels.
7. Bump chain.length to 20 in compose default; manual scalability validation
   per README (kill a broker/mongo node mid-run, confirm continuity) —
   documented, not automated, per spec's Out of Scope.

## Risks / open items (post-review)

- Hop-trace array on every envelope for 20 hops adds a small, bounded,
  symmetric serialization cost on both paths (see fairness invariant above) —
  does not perturb the benchmark.
- **TTL vs consumer lag (defect 4, second half):** a 2-minute TTL can expire a
  claim-check document mid-chain if a stage stalls (rebalance, GC pause,
  broker hiccup), producing a `Skipped` result and silently dropping that
  message from CLAIM_CHECK latency percentiles — survivorship bias understating
  claim-check tail latency. Mitigation: emit a `stage.skipped` counter tagged
  `stage`/`path`/`reason`, surface it on the dashboard cluster-status strip,
  and treat any non-zero skip rate during a run as invalidating that run's
  percentiles (documented in README). At 0.5 pairs/s with a 20-hop chain
  spanning ~1-3s, steady-state skips should be near zero; watch for skips
  during startup/rebalance windows specifically.
