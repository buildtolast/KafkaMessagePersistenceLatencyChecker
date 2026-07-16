# Handoff: next slice is Task 6 (dashboard scraper + aggregator, pure logic)

Updated: 2026-07-16 evening. Plan: `docs/plans/2026-07-15-claim-check-implementation.md`.

## State
- Tasks 0-5 done and committed (latest: `feat(consumer): kafka listener with claim-check resolution and metrics`).
- Consumer service complete: resolver unit tests + Testcontainers integration test all GREEN
  (`cd consumer-service && ./gradlew test` — 4 tests, ~10s with cached docker images).
- Remote: `origin` = git@github.com:buildtolast/KafkaMessagePersistenceLatencyChecker.git (push after each task).

## Local LLM environment (working setup)
- llama.cpp at `http://localhost:8080` (health: `/health`), model
  `unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M`, no auth.
- IT IS A REASONING MODEL: on large specs it burns the whole completion budget in
  `reasoning_content` and returns empty content ("no code block in reply").
  FIX: set `LLM_NO_THINK=1` — tools now send `chat_template_kwargs.enable_thinking=false`.
  `LLM_MAX_TOKENS` is also env-overridable (default 16384).
- Full env for every tool call:
  `export UNSLOTH_API_KEY=local UNSLOTH_BASE_URL=http://localhost:8080 UNSLOTH_MODEL='unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M' LLM_NO_THINK=1`

## Gotchas
- No root gradle wrapper; per-service `gradlew`. Run llm tools from repo root.
- Loop: `python3 tools/llm-loop.py <output_file> "<verify_cmd>" < spec.md`;
  single-shot: `./tools/llm-gen.sh <output_file> < spec.md`. Tests: single-shot only.
- Spec style that works: verbatim import block, "ALREADY EXIST, do NOT declare",
  exactly one fenced block, line cap, no wildcard imports, no checked-exception
  method refs. See `specs/gen/consumer-*.md` as templates.

## Task 6 steps (plan: "Task 6: Dashboard — Prometheus scraper + aggregator")
Pure logic, no Spring context needed in tests:
1. Write specs for MetricSample, PrometheusScraper, ComparisonModel, MetricsAggregator
   + tests PrometheusScraperTest, MetricsAggregatorTest (interfaces are in the plan).
2. TDD: generate the two test classes single-shot, confirm RED
   (`cd dashboard-service && ./gradlew test`).
3. Generate impls (records single-shot; loop PrometheusScraper and MetricsAggregator).
4. Review, `tools/token-report.py`, commit per plan.
