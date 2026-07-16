# Handoff: next slice is Task 7 (dashboard scheduler, SSE endpoint, UI)

Updated: 2026-07-16 night. Plan: `docs/plans/2026-07-15-claim-check-implementation.md`.

## State
- Tasks 0-6 done and committed. Dashboard pure logic complete: PrometheusScraper +
  MetricsAggregator, 13 tests GREEN (`cd dashboard-service && ./gradlew test`).
- Aggregator semantics (binding for Task 7): constructor `MetricsAggregator(double windowSeconds)`;
  rates from counter deltas between aggregate() calls (first call = 0); quantiles
  count-weighted across instances; mongoDocs/mongoBytes from producer CLAIM_CHECK totals;
  absent snapshot marks the instance down. ScrapeScheduler should construct it with
  windowSeconds matching the 2 s scrape rate.
- README.md exists; flip its Status table rows as Tasks 7-8 land.
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

## Task 7 steps (plan: "Task 7: Dashboard — scrape scheduler, SSE endpoint, UI")
1. Specs+TDD: StreamControllerTest first (MockWebServer stubs two fake /actuator/prometheus
   targets; /api/stats returns aggregated model; /api/stream emits at least one SSE event).
   Confirm RED. mockwebserver 4.12.0 is already in dashboard build.gradle.
2. Generate ScrapeScheduler (@Scheduled fixedRate=2000, targets from app.dashboard.targets
   comma list / DASHBOARD_TARGETS env; GET <base>/actuator/prometheus, PrometheusScraper.parse,
   MetricsAggregator.aggregate, hold latest ComparisonModel), StreamController
   (GET /api/stats JSON; GET /api/stream SseEmitter every 2 s), DashboardApplication.
   Single-shot each; loop one offender if needed.
3. Generate static/index.html + static/app.js from a UI spec quoting design spec section 7
   five zones (specs/2026-07-15-kafka-claim-check-design.md lines ~113-148). Chart.js is
   vendored at static/vendor/chart.umd.js. No verify loop for JS; browser review in Task 8.
4. Review, token-report, commit: `feat(dashboard): scrape scheduler, SSE stream, comparison UI`
