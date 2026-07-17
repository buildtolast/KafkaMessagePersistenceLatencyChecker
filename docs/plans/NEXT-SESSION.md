# Handoff: next slice is Task 8 (end-to-end compose validation, browser review)

Updated: 2026-07-16 night. Plan: `docs/plans/2026-07-15-claim-check-implementation.md`.

## State
- Tasks 0-7 done and committed. Dashboard complete: ScrapeScheduler (@Scheduled
  fixedRate=2000, targets from `app.dashboard.targets` / DASHBOARD_TARGETS, failed
  scrape omits the instance → marked down), StreamController (GET /api/stats JSON;
  GET /api/stream SSE, event NAME "stats" — client uses addEventListener('stats')),
  DashboardApplication (@EnableScheduling), static/index.html + static/app.js
  (five zones per design spec §7, Chart.js vendored). 15 tests GREEN
  (`cd dashboard-service && ./gradlew test`; StreamControllerTest uses two
  MockWebServers as fake prometheus targets).
- README Status table updated (dashboard row Done). Flip the last row when Task 8 lands.
- Remote: `origin` = git@github.com:buildtolast/KafkaMessagePersistenceLatencyChecker.git (push after each task).

## Local LLM environment (working setup)
- llama.cpp at `http://localhost:8080` (health: `/health`), model
  `unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M`, no auth.
- Reasoning model: keep `LLM_NO_THINK=1` or it burns the budget in reasoning_content.
- Full env for every tool call:
  `export UNSLOTH_API_KEY=local UNSLOTH_BASE_URL=http://localhost:8080 UNSLOTH_MODEL='unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M' LLM_NO_THINK=1`
- NOTE: dashboard's default targets in application.yml are :8080/:8081 — collides
  with the local LLM port when running the dashboard locally; set DASHBOARD_TARGETS.

## Gotchas
- No root gradle wrapper; per-service `gradlew`. Run llm tools from repo root.
- Tests: single-shot `./tools/llm-gen.sh` only; loop `python3 tools/llm-loop.py
  <file> "<verify>"` for impls. Spec templates: `specs/gen/dashboard-*.md`.
- Recurrent gen deviations to check by hand: Boot-2 `LocalServerPort` import
  (must be org.springframework.boot.test.web.server), unhandled IOException in
  @BeforeAll, ms-vs-seconds in assertions.
- SSE test holds the connection → Spring graceful shutdown waits 30 s at the end
  of the test run; harmless.

## Task 8 steps (plan: "Task 8: end-to-end validation")
1. `docker compose up --build`; wait for health; confirm producer/consumer logs.
2. Browser-review the dashboard at :8085 (all five zones live, delta badge,
   chart rolling, cluster strip); fix UI issues by editing specs + regenerating.
3. `GET /api/stats` sanity: both paths present, rates > 0, mongoDocs growing.
4. README: flip last Status row; final commit + push.
