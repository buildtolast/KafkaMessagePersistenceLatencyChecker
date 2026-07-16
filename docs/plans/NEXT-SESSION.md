# Handoff: next slice is Task 5 (consumer listener + integration test)

Updated: 2026-07-16. Plan: `docs/plans/2026-07-15-claim-check-implementation.md`.

## State
- Tasks 0-4 done and committed (latest: `feat(consumer): claim-check resolver with payload reader (TDD)`).
- Consumer package `com.codrite.claimcheck.consumer` now has: DeliveryPath, MessageEnvelope,
  PayloadReader, ResolvedMessage, ClaimCheckResolver + ClaimCheckResolverTest (GREEN).
- NO git remote configured — `git push` fails; commits are local only. Add a remote before pushing.

## Local LLM environment (this session's working setup)
- Server: llama.cpp at `http://localhost:8080` (NOT the 8888 default in the tools).
- Model served: `unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M`. No auth required.
- Route every tool call with:
  `export UNSLOTH_API_KEY=local UNSLOTH_BASE_URL=http://localhost:8080 UNSLOTH_MODEL='unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M'`
- Check `curl -s http://localhost:8080/health` (returns `{"status":"ok"}`), not `/api/health`.

## Gotchas
- There is no root gradle wrapper; each service has its own. Verify command for the loop:
  `cd consumer-service && ./gradlew test` (run llm tools from repo root).
- Loop CLI: `python3 tools/llm-loop.py <output_file> "<verify_cmd>" < spec.md`;
  single-shot: `./tools/llm-gen.sh <output_file> < spec.md`.
- `specs/gen/consumer-integration-test.md` already exists — review/adjust it, don't rewrite from scratch.

## Task 5 steps (plan lines ~197-224)
1. Generate `ConsumerIntegrationTest.java` single-shot (Testcontainers Kafka+Mongo; one inline +
   one claim-check envelope pre-seeded in `large_payloads`; assert `consumer.messages` counters
   per path tag reach 1). Confirm RED. Do NOT loop the test.
2. Generate main files single-shot in order: ConsumerMetrics -> MongoPayloadReader ->
   EnvelopeListener -> ConsumerApplication. Run suite; loop only a single remaining offender.
3. Review hard for: checked IOException on `mapper.readValue`, listener must NOT throw on
   missing doc (error log + skip), metric names/tags exactly per plan §Conventions
   (`consumer.mongo.fetch`, `consumer.processing`, `consumer.e2e.latency`, `consumer.messages`,
   `consumer.bytes`; tag `path=INLINE|CLAIM_CHECK`; timers with percentile histograms).
   E2E latency measured from `producedAtEpochNanos`.
4. `tools/token-report.py`, then commit: `feat(consumer): kafka listener with claim-check resolution and metrics`
