# Kafka Claim-Check Pattern Demo — Design Spec

**Date:** 2026-07-15
**Project:** KafkaMessagePersistenceToMongoDB
**Pattern:** Claim Check (Enterprise Integration Patterns)
**Status:** Draft — pending user approval

---

## 1. Purpose

Showcase how Kafka messages larger than 1 MB are persisted to MongoDB and referenced
by `_id` on the topic (claim check), while messages at or under 1 MB travel inline
through Kafka as usual. Everything runs in Docker via a single `docker-compose.yml`.

## 2. Success Criteria

- `docker compose up` brings up Kafka, MongoDB, producer-service, and consumer-service with no manual steps.
- Producer emits messages on a fixed schedule, alternating ~1 KB and ~2 MB payloads.
- Small payloads arrive at the consumer inline via Kafka; large payloads arrive as an `_id` reference that the consumer resolves from MongoDB.
- Consumer logs, for every message: message ID, delivery mode (`INLINE` / `CLAIM_CHECK`), payload size in bytes, and end-to-end latency.
- All unit/integration tests pass; coverage ≥ 80% on the routing and resolution logic.

## 3. Architecture

```
+------------------+        Kafka topic: messages         +------------------+
| producer-service | ------------------------------------> | consumer-service |
|  (Spring Boot)   |   envelope JSON (inline OR _id ref)   |  (Spring Boot)   |
+---------+--------+                                        +--------+---------+
          | payload > 1 MB: insert document                          | envelope has mongoId:
          v                                                          v findById
      +-------------------------- MongoDB: large_payloads --------------------+
```

### Containers (docker-compose.yml)

| Service | Image / build | Notes |
|---|---|---|
| kafka | `apache/kafka:latest` (KRaft, single node) | No ZooKeeper. Broker `max.message.bytes` left at default (~1 MB) — the claim check is precisely what avoids raising it. Topic `messages` auto-created (1 partition). |
| mongodb | `mongo:7` | Database `claimcheck`, collection `large_payloads`. No auth for the demo. |
| producer-service | built from `producer-service/Dockerfile` | Spring Boot 3.x, Java 21, multi-stage build. |
| consumer-service | built from `consumer-service/Dockerfile` | Spring Boot 3.x, Java 21, multi-stage build. |

Startup ordering via `depends_on` with healthchecks on kafka and mongodb.

## 4. Message Contract (envelope)

Every Kafka record on topic `messages` is the same JSON envelope; exactly one of
`payload` / `mongoId` is non-null:

```json
{
  "messageId": "uuid",
  "createdAt": "ISO-8601 instant",
  "payloadSizeBytes": 2097152,
  "payload": null,          // inline body when size <= 1 MB, else null
  "mongoId": "665f1a..."    // MongoDB _id (hex string) when size > 1 MB, else null
}
```

Threshold constant: `CLAIM_CHECK_THRESHOLD_BYTES = 1_048_576` (1 MiB), measured on
the UTF-8 byte length of the payload string. Configurable via
`app.claim-check.threshold-bytes` property; default 1 MiB.

**Edge case:** a payload of exactly 1 MiB is inline by contract, but envelope JSON
overhead (~150 bytes) would push the record past the producer's default
`max.request.size` (1,048,576). The producer sets
`spring.kafka.producer.properties.max.request.size=1150000` so the contract holds
at the boundary without changing the broker default. The demo generator (1 KB /
2 MB) never lands in this band; the boundary is exercised in unit tests only.

## 5. Producer Service

- **Scheduled generator** (`@Scheduled`, fixed delay 5 s): alternates between a
  ~1 KB and a ~2 MB synthetic JSON text payload (repeated lorem-ipsum-style block,
  deterministic sizes).
- **Routing logic** (the core unit under test — `ClaimCheckRouter`):
  - size ≤ threshold → envelope with `payload` set, publish to Kafka.
  - size > threshold → insert `{_id, payload, sizeBytes, createdAt}` into
    `large_payloads`, then publish envelope with `mongoId` set.
- Mongo insert happens **before** the Kafka send; if the Kafka send fails the
  orphaned document is acceptable for the demo (documents are kept forever anyway).
- Logs each send: messageId, mode, size.

## 6. Consumer Service

- `@KafkaListener` on topic `messages`, consumer group `claim-check-demo`.
- **Resolution logic** (`ClaimCheckResolver`):
  - `payload` present → use it directly (mode `INLINE`).
  - `mongoId` present → `findById` in `large_payloads`; missing document is logged
    as an error and the message is skipped (no retry storm).
- Logs: messageId, mode, resolved payload size, latency (`now - createdAt`).

## 7. Data Model (MongoDB)

Collection `large_payloads`, plain documents (no GridFS — accepted limitation:
payloads must stay under MongoDB's 16 MB BSON cap; generator max is 2 MB):

```
{ _id: ObjectId, payload: String, sizeBytes: Long, createdAt: Date }
```

Retention: **keep forever** (explicit decision — no TTL, no consumer-side delete).
Supports replay and re-consumption.

## 8. Project Layout

```
KafkaMessagePersistenceToMongoDB/
├── docker-compose.yml
├── specs/                      # this spec + per-file generation specs
├── tools/                      # llm-gen.sh, llm-loop.py, token-report.py
├── producer-service/           # Gradle, Spring Boot 3.x, Java 21
│   └── src/main|test/java/com/codrite/claimcheck/producer/...
└── consumer-service/
    └── src/main|test/java/com/codrite/claimcheck/consumer/...
```

Key classes per service stay small and testable: `MessageEnvelope` (shared shape,
duplicated per service — no shared module for a demo), `ClaimCheckRouter` /
`ClaimCheckResolver` (pure logic, unit-tested), thin Kafka/Mongo adapters.

## 9. Testing

- **Unit (TDD, RED first):** `ClaimCheckRouter` (boundary: exactly 1 MiB is inline;
  1 MiB + 1 byte is claim-checked), `ClaimCheckResolver` (inline, mongoId,
  missing-document paths), envelope serialization round-trip.
- **Integration:** Testcontainers (Kafka + MongoDB) — one end-to-end test per path:
  small message inline; large message stored and resolved.
- Verify command for the loop: `./gradlew test` per service.

## 10. Implementation Routing (mandated — CLAUDE.md §12)

- **Bootstrap (Opus/orchestrator, one-time):** Gradle builds, Dockerfiles,
  docker-compose.yml, `tools/` scripts copied from `~/.claude/tools/llm-loop/`,
  directory scaffolding.
- **All code generation → local LLM** (Unsloth Studio, `http://localhost:8888`):
  tests first via `llm-gen.sh` single-shot (confirm RED), implementation via
  `llm-loop.py` with `./gradlew test` as the verify gate (to GREEN).
- Per-file generation specs live in `specs/`; each demands exactly one fenced code
  block, names sibling signatures ("do NOT redefine"), and stays under ~150 lines
  of output.
- Opus reviews all generated output before acceptance; `token-report.py` runs after
  each completed step.
- Fallback if the local LLM is unreachable: Sonnet subagent for codegen, stated
  explicitly.

## 11. Out of Scope

- Authentication (Kafka, Mongo), TLS.
- Payload cleanup / TTL.
- Multiple partitions, consumer scaling, schema registry.
- Payloads over 15 MB (BSON cap of the plain-collection choice).
- Compression as an alternative to the claim check (worth knowing it exists; not built).
