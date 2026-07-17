# Handoff: plan complete (Tasks 0-8 done)

Updated: 2026-07-16 night. Plan: `docs/plans/2026-07-15-claim-check-implementation.md`.

## State
- ALL TASKS DONE. Full stack validated end-to-end with `docker compose up --build`:
  all 11 containers healthy, dashboard verified live in the browser at :8085
  (hero strip, delta badge, rolling chart, segment bars, cluster strip all live),
  `/api/stats` sane (steady state: INLINE p95 ~36 ms vs CLAIM_CHECK ~46 ms at
  5 pairs/s x 2 MiB; lag 0).
- Ports: producer :8090, consumers :8091 (moved off 8080/8081 to avoid the local
  LLM port clash), dashboard :8085 host-mapped.
- Fixes landed during Task 8 (all via spec + llm-loop):
  - producer/consumer had no spring-boot-starter-web → no actuator HTTP at all;
    swapped `spring-boot-starter` for `spring-boot-starter-web` in both build.gradle.
  - EnvelopeListener compared producedAtEpochNanos against System.nanoTime()
    (arbitrary origin → negative → Micrometer silently dropped every e2e sample);
    now epoch-now minus epoch-produced, clamped with Math.max(0,…) for clock skew.
  - e2e percentiles: the new Prometheus client cannot emit quantiles on histogram
    types — e2e timer now uses publishPercentiles(0.5,0.95,0.99) ONLY (no
    publishPercentileHistogram, and the yaml percentiles-histogram entry for
    consumer.e2e.latency was removed; keeping both silently drops the quantiles).
  - Clean-code pass: ScrapeScheduler restores the interrupt flag, debug-logs
    failed scrapes, and fails fast on empty targets.

## Local LLM environment (working setup)
- llama.cpp at `http://localhost:8080` (health `/health`), model
  `unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M`, `LLM_NO_THINK=1` required.
- Env: `export UNSLOTH_API_KEY=local UNSLOTH_BASE_URL=http://localhost:8080 UNSLOTH_MODEL='unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M' LLM_NO_THINK=1`

## Possible follow-ups (not planned)
- Dashboard UI niceties from design spec §7 not yet wired: header load-settings
  readout, per-path sparklines in the hero cards, expandable per-instance tiles,
  kafka-broker/mongo-RS state in the cluster strip (needs extra scrape sources).
- Storage: keep-forever retention grows ~600 MB/min at defaults; consider a
  compose profile with a lower rate for long runs.
- Remote: `origin` = git@github.com:buildtolast/KafkaMessagePersistenceLatencyChecker.git
