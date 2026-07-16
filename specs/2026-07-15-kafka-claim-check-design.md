# Kafka Claim-Check Pattern Demo — Design Spec

**Date:** 2026-07-15 (rev 2 — scalability + observability)
**Project:** KafkaMessagePersistenceToMongoDB
**Pattern:** Claim Check (Enterprise Integration Patterns)
**Status:** Draft — pending user approval

---

## 1. Purpose

Showcase and **measure** the claim-check pattern: Kafka messages larger than 2 MB are
persisted to MongoDB and referenced by `_id` on the topic; messages at or under 2 MB
travel inline through Kafka. The system runs as a clustered Docker Compose deployment
(3-node Kafka, 3-node MongoDB replica set, 3 consumer instances) with **in-app
observability** and a **real-time UI dashboard** giving an apples-to-apples
comparison of the two transport paths.

## 2. Success Criteria

- `docker compose up` brings up the full topology (11 containers) with no manual steps.
- Producer emits **forced-path pairs**: identical ~2 MB payloads sent down BOTH paths
  (inline and claim-check) at a configurable rate, so the comparison is same-size,
  different-transport.
- Dashboard at `http://localhost:8085` shows, live, per path (INLINE vs CLAIM_CHECK):
  throughput (msg/s, MB/s), end-to-end latency p50/p95/p99, and segmented timings.
- Kill one Kafka broker or one Mongo node → system keeps flowing (RF 3 / replica set).
- All unit/integration tests pass; coverage ≥ 80% on routing, resolution, and metrics logic.

## 3. Architecture

```
                       Kafka cluster (3 brokers, KRaft, RF=3)
                          topic: messages (3 partitions)
producer-service ──────────────────────────────────────────────► consumer-service x3
   │  CLAIM_CHECK path: insert payload                                │  mongoId → findById
   ▼                                                                  ▼
              MongoDB replica set (mongo1 primary, mongo2/3 secondaries)
                        db: claimcheck, coll: large_payloads

producer + consumers expose /actuator/prometheus ──► dashboard-service (scrape 2s)
                                                            │ SSE
                                                            ▼
                                              browser: live comparison charts
```

### Containers (docker-compose.yml)

| Service | Count | Image / build | Notes |
|---|---|---|---|
| kafka1..3 | 3 | `apache/kafka:latest` | KRaft combined mode (controller+broker quorum of 3). Topic `messages`: 3 partitions, RF 3, `min.insync.replicas=2`. Broker `message.max.bytes=3145728` (3 MB) so a 2 MB inline payload + envelope overhead fits. |
| mongo1..3 | 3 | `mongo:7` | Replica set `rs0`, initialized by a one-shot init container. Writes use `w:majority`. No auth (demo). |
| producer-service | 1 | Spring Boot 3.x / Java 21 | Scheduled generator + router + metrics. `max.request.size=3145728`, `acks=all`. |
| consumer-service | 3 | Spring Boot 3.x / Java 21 | Same image, 3 replicas in one consumer group; each gets one partition. |
| dashboard-service | 1 | Spring Boot 3.x / Java 21 | Scrapes all app instances' Actuator endpoints, aggregates, serves the UI + SSE stream on port 8085. |

Startup ordering via healthchecks (`depends_on: condition: service_healthy`).

## 4. Message Contract (envelope)

Every Kafka record on topic `messages` is the same JSON envelope; exactly one of
`payload` / `mongoId` is non-null:

```json
{
  "messageId": "uuid",
  "pairId": "uuid",          // same for the inline/claim-check twin pair
  "createdAt": "ISO-8601 instant (producer clock, epoch nanos also included)",
  "producedAtNanos": 123,     // System.nanoTime-free wall clock in nanos for latency calc
  "payloadSizeBytes": 2097152,
  "forcedPath": "INLINE",    // INLINE | CLAIM_CHECK | null (threshold-driven)
  "payload": null,
  "mongoId": "665f1a..."
}
```

- Threshold constant: `CLAIM_CHECK_THRESHOLD_BYTES = 2_097_152` (2 MiB), property
  `app.claim-check.threshold-bytes`. Measured on UTF-8 byte length of the payload.
- **Forced-path flag:** the generator sets `forcedPath` to route an over- or
  under-threshold payload down a specific path for the comparison workload. Routing
  precedence: `forcedPath` if set, else threshold rule. All containers run on the
  same host, so producer/consumer wall clocks are directly comparable for
  end-to-end latency.

## 5. Producer Service

- **Pair generator** (`@Scheduled`, rate configurable via `app.load.pairs-per-second`,
  default 5 pairs/s): each tick creates ONE ~2 MB payload and sends it twice with the
  same `pairId` — once `forcedPath=INLINE`, once `forcedPath=CLAIM_CHECK`.
  Payload size configurable (`app.load.payload-bytes`, default 2,097,152).
- **Routing logic** (`ClaimCheckRouter`, pure and unit-tested): forcedPath override,
  else size > threshold → Mongo insert (`w:majority`) then envelope with `mongoId`;
  else inline envelope.
- Mongo insert happens before the Kafka send; orphaned documents on send failure are
  acceptable (retention is keep-forever anyway).
- **Metrics recorded** (Micrometer, tag `path=INLINE|CLAIM_CHECK`):
  - `producer.mongo.insert` (Timer) — claim-check path only
  - `producer.kafka.send` (Timer) — send-to-ack
  - `producer.messages` (Counter), `producer.bytes` (Counter)

## 6. Consumer Service

- `@KafkaListener`, group `claim-check-demo`, 3 instances / 3 partitions.
- **Resolution logic** (`ClaimCheckResolver`): inline payload used directly;
  `mongoId` → `findById` (secondary reads off; primary read for correctness);
  missing document → log error, skip (no retry storm).
- **Metrics recorded** (tag `path=...`):
  - `consumer.mongo.fetch` (Timer) — claim-check path only
  - `consumer.processing` (Timer) — deserialize + resolve total
  - `consumer.e2e.latency` (Timer) — `now - producedAt` from the envelope
  - `consumer.messages` (Counter), `consumer.bytes` (Counter)

## 7. Observability & Dashboard (in-app, no third-party observability stack)

- **Instrumentation:** Micrometer (bundled with Spring Boot Actuator) with
  percentile histograms enabled for the timers above; exposed at
  `/actuator/prometheus` and `/actuator/health` on every app instance.
- **dashboard-service:**
  - Scrapes each instance's `/actuator/prometheus` every 2 s (instance list from
    static compose DNS names; consumer replicas resolved via Docker DNS
    `tasks.consumer-service` or an explicit configured list).
  - Aggregates across instances into one comparison model:
    per path — msg/s, MB/s, e2e p50/p95/p99, mongo-insert avg, kafka-send avg,
    mongo-fetch avg, processing avg, totals, per-consumer partition balance.
  - Pushes snapshots to the browser via **SSE** (`/api/stream`); also `GET /api/stats`
    for scripting.
  - **UI:** single static `index.html` served by the same app — vanilla JS + Chart.js
    (vendored into `src/main/resources/static/`, no CDN at runtime). Layout contract
    (comparison-centric, top to bottom):
    1. **Header bar** — title, load settings (payload size, pairs/s, window),
       live indicator, pause button.
    2. **Hero comparison strip** — INLINE and CLAIM_CHECK face each other:
       e2e p95 as the headline number, msg/s + MB/s subline, a small latency
       sparkline per path, and centered between them an **overhead delta badge**
       (+ms and +% p95, claim-check relative to inline).
    3. **Latency percentiles chart** — rolling 5-minute Chart.js line chart,
       per path: p50 solid, p99 dashed (tail behavior is the interesting part).
    4. **"Where the time goes"** — horizontal segment bars per path, bar width
       proportional to total average latency; segments labeled inline
       (inline: kafka-send, processing; claim-check: mongo-insert, kafka-send,
       mongo-fetch, processing). Plus a Mongo storage counter (total docs, GB)
       given keep-forever retention.
    5. **Cluster status strip** — one-line footer: kafka brokers up (n/3),
       mongo replica-set state + primary, producer rate, each consumer's
       partition/rate/lag with a warning icon when lagging. Clicking the strip
       expands per-instance detail tiles.
- No Prometheus server, Grafana, or agents — everything lives inside the apps.

## 8. Data Model (MongoDB)

Replica set `rs0`, database `claimcheck`, collection `large_payloads` (plain
documents — accepted limit: payloads < 16 MB BSON cap; workload uses ~2 MB):

```
{ _id: ObjectId, payload: String, sizeBytes: Long, createdAt: Date }
```

Retention: **keep forever** (no TTL, no consumer-side delete) — supports replay.
Note for long scalability runs: ~2 MB × 5 pairs/s ≈ 600 MB/min of Mongo growth;
the compose volume should have headroom, and rate is tunable downward.

## 9. Project Layout

```
KafkaMessagePersistenceToMongoDB/
├── docker-compose.yml
├── specs/                      # this spec + per-file generation specs
├── tools/                      # llm-gen.sh, llm-loop.py, token-report.py
├── producer-service/           # Gradle, Spring Boot 3.x, Java 21
├── consumer-service/
└── dashboard-service/          # aggregator + static UI
```

`MessageEnvelope` duplicated per service (no shared module for a demo).
Core pure-logic classes stay small: `ClaimCheckRouter`, `ClaimCheckResolver`,
`MetricsAggregator` (dashboard), `PrometheusScraper` (dashboard).

## 10. Testing

- **Unit (TDD, RED first):**
  - `ClaimCheckRouter`: boundary (exactly 2 MiB inline; +1 byte claim-checked),
    forcedPath override in both directions.
  - `ClaimCheckResolver`: inline, mongoId, missing-document paths.
  - `MetricsAggregator`: merging multi-instance Prometheus samples into the
    comparison model (percentile handling, counter rate calculation).
  - Envelope serialization round-trip.
- **Integration:** Testcontainers (single Kafka + single Mongo suffice for
  correctness): one end-to-end test per path; one dashboard scrape/aggregate test
  against a stubbed metrics endpoint.
- Verify command for the loop: `./gradlew test` per service.
- **Scalability validation (manual, documented in README):** run at increasing
  `pairs-per-second`, watch the dashboard; kill `kafka2` and `mongo2` mid-run and
  observe continuity.

## 11. Implementation Routing (mandated — CLAUDE.md §12)

- **Bootstrap (orchestrator, one-time):** Gradle builds, Dockerfiles,
  docker-compose.yml (incl. Kafka quorum + Mongo replica-set init), `tools/` scripts
  copied from `~/.claude/tools/llm-loop/`, vendored Chart.js, directory scaffolding.
- **All code generation → local LLM** (Unsloth Studio, `http://localhost:8888`):
  tests first via `llm-gen.sh` single-shot (confirm RED), implementation via
  `llm-loop.py` with `./gradlew test` as the verify gate (to GREEN).
- Per-file generation specs live in `specs/`; each demands exactly one fenced code
  block, names sibling signatures ("do NOT redefine"), stays under ~150 output lines.
- Opus reviews all generated output before acceptance; `token-report.py` runs after
  each completed step.
- Fallback if the local LLM is unreachable: Sonnet subagent for codegen, stated explicitly.

## 12. Out of Scope

- Authentication (Kafka, Mongo), TLS.
- Payload cleanup / TTL.
- Schema registry, Kafka Streams, sharded Mongo.
- Third-party observability infrastructure (Prometheus server, Grafana, Datadog).
- Payloads over 15 MB (BSON cap of the plain-collection choice).
- Automated load-test harness (rates are config-driven; runs are manual).
