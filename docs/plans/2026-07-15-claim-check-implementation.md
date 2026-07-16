# Kafka Claim-Check Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **ROUTING OVERRIDE (CLAUDE.md §12, mandated by spec §11):** All code generation
> goes through the local-LLM loop, not hand-written by the executing agent.
> Tests: `tools/llm-gen.sh <spec> <output-file>` single-shot, confirm RED.
> Implementation: `tools/llm-loop.py --spec <spec> --target <file> --verify "<gradle cmd>"` to GREEN.
> The executing agent writes generation specs, reviews output, and commits.
> Bootstrap (Task 0) is the one hand-written exception. Run `tools/token-report.py`
> after each task. If `curl -s http://localhost:8888/api/health` fails, state the
> fallback and route codegen to a Sonnet subagent instead.

**Goal:** Dockerized demo measuring Kafka inline vs Mongo claim-check transport for 2 MB messages, with clustered Kafka/Mongo and a live comparison dashboard.

**Architecture:** Three Spring Boot services (producer, 3x consumer, dashboard) over a 3-broker KRaft Kafka cluster and 3-member Mongo replica set. Producer emits forced-path pairs; consumers resolve claim checks; dashboard scrapes Actuator/Prometheus endpoints and streams an aggregated comparison over SSE.

**Tech Stack:** Java 21, Spring Boot 3.3.x, Spring Kafka, Spring Data MongoDB, Micrometer + Actuator (prometheus registry), Gradle 8 (wrapper), Testcontainers, Docker Compose, vanilla JS + vendored Chart.js.

## Global Constraints

- Threshold: `CLAIM_CHECK_THRESHOLD_BYTES = 2_097_152` (2 MiB), property `app.claim-check.threshold-bytes`.
- Kafka: topic `messages`, 3 partitions, RF 3, `min.insync.replicas=2`; broker `message.max.bytes=3145728`; producer `max.request.size=3145728`, `acks=all`.
- Mongo: replica set `rs0`, db `claimcheck`, collection `large_payloads`, writes `w:majority`, keep-forever retention.
- Consumer group: `claim-check-demo`.
- Java 21, Spring Boot 3.3.x for all three services; base package `com.codrite.claimcheck.<service>`.
- Micrometer metric names exactly as spec §5/§6 (`producer.mongo.insert`, `producer.kafka.send`, `producer.messages`, `producer.bytes`, `consumer.mongo.fetch`, `consumer.processing`, `consumer.e2e.latency`, `consumer.messages`, `consumer.bytes`), all tagged `path=INLINE|CLAIM_CHECK`, timers with percentile histograms.
- Dashboard on port 8085; producer 8080; consumers 8081 (container-internal; not host-mapped per replica).
- No CDN at runtime: Chart.js vendored into `dashboard-service/src/main/resources/static/vendor/chart.umd.js`.
- Generation-spec discipline: exactly ONE fenced code block, sibling signatures named with "ALREADY EXISTS — do NOT redefine", ≤150 output lines per file, exact output path stated.
- Coverage ≥ 80% on router/resolver/aggregator logic; verify command per service: `./gradlew test`.
- Commit after every task (conventional commits, no attribution footer).

## File Structure

```
KafkaMessagePersistenceToMongoDB/
├── docker-compose.yml
├── tools/{llm-gen.sh,llm-loop.py,token-report.py}
├── specs/gen/                        # per-file generation specs (one .md per generated file)
├── producer-service/
│   ├── build.gradle, settings.gradle, Dockerfile
│   └── src/{main,test}/java/com/codrite/claimcheck/producer/
│       ├── MessageEnvelope.java, DeliveryPath.java
│       ├── ClaimCheckRouter.java, PayloadStore.java
│       ├── MongoPayloadStore.java, EnvelopePublisher.java
│       ├── PairGenerator.java, ProducerMetrics.java, ProducerApplication.java
├── consumer-service/
│   ├── build.gradle, settings.gradle, Dockerfile
│   └── src/{main,test}/java/com/codrite/claimcheck/consumer/
│       ├── MessageEnvelope.java, DeliveryPath.java (duplicated by design)
│       ├── ClaimCheckResolver.java, PayloadReader.java, ResolvedMessage.java
│       ├── MongoPayloadReader.java, EnvelopeListener.java, ConsumerMetrics.java, ConsumerApplication.java
└── dashboard-service/
    ├── build.gradle, settings.gradle, Dockerfile
    └── src/main/java/com/codrite/claimcheck/dashboard/
        ├── PrometheusScraper.java, MetricSample.java
        ├── MetricsAggregator.java, ComparisonModel.java
        ├── ScrapeScheduler.java, StreamController.java, DashboardApplication.java
    └── src/main/resources/static/{index.html, app.js, vendor/chart.umd.js}
```

---

### Task 0: Bootstrap (hand-written — the one exception)

**Files:** Create everything non-Java: `tools/` (copied), three Gradle projects with empty `src` trees, Dockerfiles (multi-stage `gradle:8-jdk21` → `eclipse-temurin:21-jre`), `docker-compose.yml` (kafka1..3 KRaft quorum; mongo1..3 + one-shot `mongo-init` container running `rs.initiate` with the 3 hosts; topic-init container creating `messages` 3p/RF3/min.insync=2; producer; consumer ×3 via `deploy.replicas: 3`… compose non-swarm: use three explicit services `consumer1..3` sharing one build; dashboard with port `8085:8085`), `.gitignore`, vendored Chart.js, `specs/gen/` dir.

- [ ] **Step 0.1:** `mkdir -p tools && cp ~/.claude/tools/llm-loop/{llm-gen.sh,llm-loop.py,token-report.py} tools/ && chmod +x tools/*`
- [ ] **Step 0.2:** Health check `curl -s --max-time 3 http://localhost:8888/api/health` → expect `"status":"healthy"`; confirm `UNSLOTH_API_KEY` is set (`test -n "$UNSLOTH_API_KEY"`).
- [ ] **Step 0.3:** Scaffold the three Gradle projects. Each `build.gradle`: plugins `java`, `org.springframework.boot 3.3.x`, `io.spring.dependency-management`; deps per service —
  producer: `spring-boot-starter`, `spring-kafka`, `spring-boot-starter-data-mongodb`, `spring-boot-starter-actuator`, `micrometer-registry-prometheus`, `spring-boot-starter-json`; test: `spring-boot-starter-test`, `spring-kafka-test`, `org.testcontainers:kafka`, `org.testcontainers:mongodb`, `org.testcontainers:junit-jupiter`.
  consumer: same as producer.
  dashboard: `spring-boot-starter-web`, `spring-boot-starter-actuator`, `micrometer-registry-prometheus`; test: `spring-boot-starter-test`, `com.squareup.okhttp3:mockwebserver`.
  Generate wrappers: `gradle wrapper --gradle-version 8.10` in each (or copy one wrapper).
- [ ] **Step 0.4:** Write `application.yml` per service with the Global Constraints values (thresholds, kafka client props, mongo URI `mongodb://mongo1:27017,mongo2:27017,mongo3:27017/claimcheck?replicaSet=rs0&w=majority`, actuator exposure `health,prometheus,metrics`, percentile histograms for the spec'd timers via `management.metrics.distribution.percentiles-histogram`).
- [ ] **Step 0.5:** Write `docker-compose.yml` + Dockerfiles as above; vendor Chart.js: `curl -Lo dashboard-service/src/main/resources/static/vendor/chart.umd.js https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.js`.
- [ ] **Step 0.6:** Verify: `(cd producer-service && ./gradlew build -x test)` ×3 → BUILD SUCCESSFUL; `docker compose config -q` → exit 0.
- [ ] **Step 0.7:** Commit: `chore: bootstrap gradle projects, docker compose topology, llm-loop tooling`

---

### Task 1: Producer — envelope model

**Files:** Create `producer-service/src/main/java/.../producer/DeliveryPath.java`, `MessageEnvelope.java`; Test `producer-service/src/test/java/.../producer/MessageEnvelopeTest.java`; Specs `specs/gen/producer-envelope-test.md`, `specs/gen/producer-envelope.md`.

**Interfaces (Produces):**
```java
public enum DeliveryPath { INLINE, CLAIM_CHECK }

public record MessageEnvelope(
    String messageId, String pairId, java.time.Instant createdAt,
    long producedAtEpochNanos, long payloadSizeBytes,
    DeliveryPath forcedPath,          // nullable
    String payload,                   // nullable — exactly one of payload/mongoId set
    String mongoId) {}                // nullable
```
JSON via Jackson (`JavaTimeModule` registered), null fields omitted (`@JsonInclude(NON_NULL)` on the record).

- [ ] **Step 1.1:** Write `specs/gen/producer-envelope-test.md`: contract above verbatim; require tests for (a) JSON round-trip of an inline envelope preserving all fields, (b) round-trip of a claim-check envelope (`payload=null`, `mongoId` set), (c) serialized inline envelope contains no `"mongoId"` key. State output path, one fenced block, ≤150 lines, "DeliveryPath and MessageEnvelope DO NOT EXIST YET — reference them as if they exist, do NOT declare them."
- [ ] **Step 1.2:** `tools/llm-gen.sh specs/gen/producer-envelope-test.md producer-service/src/test/java/com/codrite/claimcheck/producer/MessageEnvelopeTest.java`
- [ ] **Step 1.3:** Confirm RED: `(cd producer-service && ./gradlew test)` → compile error "cannot find symbol MessageEnvelope". Review the test as the contract (imports, no redefinitions).
- [ ] **Step 1.4:** Write `specs/gen/producer-envelope.md` (both files in one spec is fine for a 3-line enum: generate `DeliveryPath.java` single-shot with llm-gen.sh, then loop the record): contract verbatim, "output exactly one top-level type", record with `@JsonInclude(JsonInclude.Include.NON_NULL)`.
- [ ] **Step 1.5:** `tools/llm-gen.sh` for `DeliveryPath.java`; then `tools/llm-loop.py --spec specs/gen/producer-envelope.md --target producer-service/src/main/java/com/codrite/claimcheck/producer/MessageEnvelope.java --verify "cd producer-service && ./gradlew test"` → GREEN.
- [ ] **Step 1.6:** Review generated code (checked exceptions on Jackson calls in test, static imports, no compact-constructor `this.x=`); `tools/token-report.py`.
- [ ] **Step 1.7:** Commit: `feat(producer): message envelope model with delivery path`

---

### Task 2: Producer — ClaimCheckRouter

**Files:** Create `.../producer/PayloadStore.java`, `ClaimCheckRouter.java`; Test `.../producer/ClaimCheckRouterTest.java`; Specs `specs/gen/producer-router-test.md`, `specs/gen/producer-router.md`.

**Interfaces:**
- Consumes: `MessageEnvelope`, `DeliveryPath` (Task 1 — ALREADY EXIST).
- Produces:
```java
public interface PayloadStore { String store(String payload, long sizeBytes); }

public final class ClaimCheckRouter {
  public ClaimCheckRouter(long thresholdBytes, PayloadStore store) {}
  /** Builds the envelope, storing to Mongo when routed CLAIM_CHECK. */
  public MessageEnvelope route(String messageId, String pairId, String payload, DeliveryPath forcedPath) {}
  public DeliveryPath decide(long payloadSizeBytes, DeliveryPath forcedPath) {}
}
```
`decide`: forcedPath wins when non-null; else `sizeBytes > threshold → CLAIM_CHECK` else `INLINE`. Size = `payload.getBytes(UTF_8).length`. `route` sets `createdAt=Instant.now()`, `producedAtEpochNanos = System.currentTimeMillis()*1_000_000 + nanos remainder from Instant`.

- [ ] **Step 2.1:** Write `specs/gen/producer-router-test.md` requiring tests: exactly 2 MiB → INLINE; 2 MiB+1 → CLAIM_CHECK; forced INLINE on oversize payload stays inline (store never called — use a recording fake, not Mockito); forced CLAIM_CHECK on tiny payload stores and sets mongoId with null payload; inline route leaves mongoId null and store uncalled. "MessageEnvelope and DeliveryPath ALREADY EXIST in this package — do NOT redefine."
- [ ] **Step 2.2:** `tools/llm-gen.sh specs/gen/producer-router-test.md producer-service/src/test/java/com/codrite/claimcheck/producer/ClaimCheckRouterTest.java`
- [ ] **Step 2.3:** Confirm RED (missing `ClaimCheckRouter`/`PayloadStore` symbols); review test.
- [ ] **Step 2.4:** `llm-gen.sh` the one-line `PayloadStore.java`; then `tools/llm-loop.py --spec specs/gen/producer-router.md --target .../ClaimCheckRouter.java --verify "cd producer-service && ./gradlew test"` → GREEN.
- [ ] **Step 2.5:** Review; `tools/token-report.py`.
- [ ] **Step 2.6:** Commit: `feat(producer): claim-check routing with threshold and forced-path override`

---

### Task 3: Producer — Mongo store, Kafka publisher, pair generator, metrics wiring

**Files:** Create `.../producer/MongoPayloadStore.java`, `EnvelopePublisher.java`, `ProducerMetrics.java`, `PairGenerator.java`, `ProducerApplication.java`; Test `.../producer/ProducerIntegrationTest.java` (Testcontainers); Specs `specs/gen/producer-integration-test.md`, one spec per main file.

**Interfaces:**
- Consumes: Task 1–2 types (ALREADY EXIST).
- Produces:
```java
@Component public final class MongoPayloadStore implements PayloadStore {
  public MongoPayloadStore(org.springframework.data.mongodb.core.MongoTemplate template) {}
  // inserts {_id, payload, sizeBytes, createdAt} into "large_payloads", returns _id hex
}
@Component public final class EnvelopePublisher {
  public EnvelopePublisher(org.springframework.kafka.core.KafkaTemplate<String,String> kafka,
                           com.fasterxml.jackson.databind.ObjectMapper mapper, ProducerMetrics metrics) {}
  public void publish(MessageEnvelope env) {} // topic "messages", key = pairId; times producer.kafka.send
}
@Component public final class ProducerMetrics {
  public ProducerMetrics(io.micrometer.core.instrument.MeterRegistry registry) {}
  public io.micrometer.core.instrument.Timer mongoInsert(DeliveryPath p) {}
  public io.micrometer.core.instrument.Timer kafkaSend(DeliveryPath p) {}
  public void recordMessage(DeliveryPath p, long bytes) {}
}
@Component public final class PairGenerator { // @Scheduled(fixedRateString="${app.load.tick-millis:200}")
  // each tick: build payload of ${app.load.payload-bytes:2097152}, one pairId,
  // route+publish twice: forcedPath=INLINE and forcedPath=CLAIM_CHECK
}
```
- [ ] **Step 3.1:** Generate integration test first (`llm-gen.sh`, spec `producer-integration-test.md`): `@Testcontainers` with `KafkaContainer` + `MongoDBContainer`, boots the app with overridden properties, asserts after one generator tick that (a) topic `messages` received 2 records sharing a pairId, one with payload and one with mongoId, and (b) `large_payloads` has exactly 1 document with matching `_id`. Confirm RED.
- [ ] **Step 3.2:** Generate each main file single-shot in dependency order (`ProducerMetrics` → `MongoPayloadStore` → `EnvelopePublisher` → `PairGenerator` → `ProducerApplication` with `@SpringBootApplication @EnableScheduling` and a `ClaimCheckRouter` `@Bean` from the threshold property). Each spec names all sibling signatures as ALREADY EXISTS.
- [ ] **Step 3.3:** Run `./gradlew test`. If a single file is broken, drive `llm-loop.py` at that one offender only (rest must compile). → GREEN.
- [ ] **Step 3.4:** Review hardest: metric names/tags exactly per Global Constraints; `w:majority` honored via URI; scheduler off in tests (`app.load.enabled` guard if needed — allowed one boolean property).
- [ ] **Step 3.5:** `tools/token-report.py`; Commit: `feat(producer): pair generator, mongo store, kafka publisher, metrics`

---

### Task 4: Consumer — envelope + resolver

**Files:** Create `consumer-service/src/main/java/.../consumer/{DeliveryPath,MessageEnvelope,PayloadReader,ResolvedMessage,ClaimCheckResolver}.java`; Test `.../consumer/ClaimCheckResolverTest.java`; Specs `specs/gen/consumer-resolver-test.md`, `specs/gen/consumer-resolver.md`.

**Interfaces:**
- `DeliveryPath`, `MessageEnvelope`: identical source to Task 1 (copy the reviewed producer files — deliberate duplication, do not regenerate; adjust package line only).
- Produces:
```java
public interface PayloadReader { java.util.Optional<String> fetch(String mongoId); }
public record ResolvedMessage(String messageId, DeliveryPath path, String payload, long sizeBytes) {}
public final class ClaimCheckResolver {
  public ClaimCheckResolver(PayloadReader reader) {}
  /** empty Optional when a claim-check document is missing (caller logs+skips). */
  public java.util.Optional<ResolvedMessage> resolve(MessageEnvelope env) {}
}
```
- [x] **Step 4.1:** Copy the two model files from producer (change package), commit-stage them.
- [x] **Step 4.2:** Generate resolver test (`llm-gen.sh`): inline envelope → ResolvedMessage with path INLINE and reader uncalled; mongoId envelope → reader fetch called, path CLAIM_CHECK, payload from reader; missing document → `Optional.empty()`. Recording fake reader, no Mockito. Confirm RED.
- [x] **Step 4.3:** `llm-gen.sh` `PayloadReader.java` + `ResolvedMessage.java`; `llm-loop.py` for `ClaimCheckResolver.java` with verify `cd consumer-service && ./gradlew test` → GREEN.
- [x] **Step 4.4:** Review; `tools/token-report.py`; Commit: `feat(consumer): claim-check resolver with missing-document handling`

---

### Task 5: Consumer — listener, Mongo reader, metrics, integration test

**Files:** Create `.../consumer/{MongoPayloadReader,ConsumerMetrics,EnvelopeListener,ConsumerApplication}.java`; Test `.../consumer/ConsumerIntegrationTest.java`; Specs one per file + `specs/gen/consumer-integration-test.md`.

**Interfaces (Produces):**
```java
@Component public final class MongoPayloadReader implements PayloadReader {
  public MongoPayloadReader(org.springframework.data.mongodb.core.MongoTemplate template) {} // findById in "large_payloads", times consumer.mongo.fetch
}
@Component public final class ConsumerMetrics {
  public ConsumerMetrics(io.micrometer.core.instrument.MeterRegistry registry) {}
  public void recordE2e(DeliveryPath p, java.time.Duration d) {}
  public void recordProcessing(DeliveryPath p, java.time.Duration d) {}
  public void recordMessage(DeliveryPath p, long bytes) {}
}
@Component public final class EnvelopeListener {
  // @KafkaListener(topics="messages", groupId="claim-check-demo")
  // deserialize -> resolve -> metrics (e2e from producedAtEpochNanos) -> log mode/size/latency; missing doc: error log, no throw
}
```
- [x] **Step 5.1:** Generate integration test first: Testcontainers Kafka+Mongo; seed one 3-byte inline envelope and one claim-check envelope whose payload was pre-inserted into `large_payloads`; assert both are consumed and `consumer.messages` counters for both path tags reach 1 (query `MeterRegistry`). Confirm RED.
- [x] **Step 5.2:** Generate main files single-shot in order (`ConsumerMetrics` → `MongoPayloadReader` → `EnvelopeListener` → `ConsumerApplication`); loop only a single offender if the suite fails. → GREEN.
- [x] **Step 5.3:** Review (checked IOException on `mapper.readValue`, no listener throw on missing doc); `tools/token-report.py`.
- [x] **Step 5.4:** Commit: `feat(consumer): kafka listener with claim-check resolution and metrics`

---

### Task 6: Dashboard — Prometheus scraper + aggregator (pure logic)

**Files:** Create `dashboard-service/src/main/java/.../dashboard/{MetricSample,PrometheusScraper,ComparisonModel,MetricsAggregator}.java`; Tests `.../dashboard/PrometheusScraperTest.java`, `MetricsAggregatorTest.java`; Specs 4 gen specs.

**Interfaces (Produces):**
```java
public record MetricSample(String name, java.util.Map<String,String> tags, double value) {}
public final class PrometheusScraper {
  /** Parses Prometheus text exposition format; ignores comments and NaN. */
  public static java.util.List<MetricSample> parse(String body) {}
}
public record ComparisonModel(
    java.util.Map<String, PathStats> byPath,           // keys "INLINE","CLAIM_CHECK"
    java.util.List<InstanceHealth> instances,
    long mongoDocs, double mongoBytes) {
  public record PathStats(double msgPerSec, double mbPerSec,
      double e2eP50Ms, double e2eP95Ms, double e2eP99Ms,
      double mongoInsertAvgMs, double kafkaSendAvgMs,
      double mongoFetchAvgMs, double processingAvgMs) {}
  public record InstanceHealth(String name, boolean up, double msgPerSec, double lag) {}
}
public final class MetricsAggregator {
  /** snapshots: instanceName -> samples; windowSeconds for rate calc from counter deltas vs previous call. */
  public synchronized ComparisonModel aggregate(java.util.Map<String, java.util.List<MetricSample>> snapshots) {}
}
```
- [x] **Step 6.1:** Generate `PrometheusScraperTest` (parse a literal 15-line exposition sample: counters, timer `_sum`/`_count`, histogram quantile lines, a `# HELP` comment, a `NaN`) — confirm RED; `llm-loop.py` the scraper → GREEN. Commit: `feat(dashboard): prometheus text parser`.
- [x] **Step 6.2:** Generate `MetricsAggregatorTest` (two consumer instances' samples merge: counter rates from deltas across two aggregate calls, quantiles averaged across instances weighted by count, per-path stats keyed by `path` tag, instance down = absent snapshot) — confirm RED; generate the two records single-shot; `llm-loop.py` the aggregator → GREEN.
- [x] **Step 6.3:** Review percentile-merge math by hand; `tools/token-report.py`; Commit: `feat(dashboard): multi-instance metrics aggregation`

---

### Task 7: Dashboard — scrape scheduler, SSE endpoint, UI

**Files:** Create `.../dashboard/{ScrapeScheduler,StreamController,DashboardApplication}.java`, `static/index.html`, `static/app.js`; Test `.../dashboard/StreamControllerTest.java` (MockWebServer stubs two fake `/actuator/prometheus` targets); Specs one per file.

**Interfaces:**
- Consumes: Task 6 types (ALREADY EXIST).
- Produces:
```java
@Component public final class ScrapeScheduler { // @Scheduled(fixedRate=2000)
  // targets from property app.dashboard.targets (comma list of base URLs);
  // GET <base>/actuator/prometheus, parse, aggregate, hold latest ComparisonModel
  public ComparisonModel latest() {}
}
@RestController public final class StreamController {
  // GET /api/stats -> ComparisonModel JSON
  // GET /api/stream -> text/event-stream, pushes latest() every 2s (SseEmitter)
}
```
- UI contract = spec §7 layout (header bar, hero strip with overhead delta badge + sparklines, p50/p99 chart, proportional segment bars + Mongo storage counter, cluster status strip with expandable tiles). `app.js` consumes `/api/stream` via `EventSource`, renders with vendored Chart.js. Generate `index.html` and `app.js` via `llm-gen.sh` (no verify loop for JS; review in browser at Task 8).
- [ ] **Step 7.1:** Generate `StreamControllerTest` (stats endpoint returns aggregated model from two MockWebServer targets; stream endpoint emits at least one event) — confirm RED.
- [ ] **Step 7.2:** Generate `ScrapeScheduler`, `StreamController`, `DashboardApplication` (single-shot, loop one offender if needed) → GREEN.
- [ ] **Step 7.3:** Generate `index.html` + `app.js` from a UI spec quoting spec §7's five zones verbatim.
- [ ] **Step 7.4:** Review; `tools/token-report.py`; Commit: `feat(dashboard): scrape scheduler, SSE stream, comparison UI`

---

### Task 8: End-to-end bring-up and validation

- [ ] **Step 8.1:** `docker compose build && docker compose up -d`; wait for healthchecks: `docker compose ps` → all healthy.
- [ ] **Step 8.2:** `curl -s localhost:8085/api/stats` → JSON with both path keys and nonzero msgPerSec after ~30 s.
- [ ] **Step 8.3:** Open dashboard in browser at `http://localhost:8085`, verify all five UI zones render and update live (use the in-app browser; check console for errors).
- [ ] **Step 8.4:** Failover check: `docker compose stop kafka2 mongo2` → flow continues (dashboard status strip shows degradation); `docker compose start kafka2 mongo2`.
- [ ] **Step 8.5:** Write `README.md`: run instructions, dashboard tour, tuning properties (`app.load.*`), the ~600 MB/min Mongo growth note, failover recipe.
- [ ] **Step 8.6:** `tools/token-report.py` final summary; Commit: `docs: readme and e2e validation notes`

---

## Self-review notes

- Spec coverage: §3 topology → Task 0; §4 envelope → Task 1; §5 producer → Tasks 2–3; §6 consumer → Tasks 4–5; §7 observability/dashboard → Tasks 6–7 (+ UI zones in 7.3); §10 routing → header override + per-task loop steps; success criteria → Task 8.
- Types cross-checked: `DeliveryPath`, `MessageEnvelope` fields, `PayloadStore.store`, `PayloadReader.fetch`, `ComparisonModel` names are consistent across tasks.
- Known deviation from classic step granularity: implementation code is specified as contracts + generation specs rather than inline Java, per the mandated routing model; the contracts above are the binding signatures.
