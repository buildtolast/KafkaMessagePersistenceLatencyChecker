# Implementation Plan: SRE Agent (issue #2)

Source: specs/2026-07-18-sre-agent-spec.md (GitHub issue #2, ready-for-agent)

## Codegen routing

Local LLM at `http://127.0.0.1:8080` on the host (llama.cpp, confirmed healthy
this session) via `tools/llm-gen.sh`/`llm-loop.py` for building this feature's
own code, same as the relay-chain work. Fable review before acceptance for
non-trivial files. Note: this is distinct from the SRE agent's own *runtime*
calls to the same LLM, which must address it as `host.docker.internal:8080`
from inside its container (round-1 fix, see below).

## Plan review (Fable) — round 1 findings folded in below

Verdict: needs revision. Verified against the actual repo (Dockerfile pattern,
package naming, `ScrapeScheduler`'s real constructor/schedule, compose service
names, and — critically — confirmed **no existing service has any bind
mount today**, all are build-only images). Found: (1) `127.0.0.1:8080` is
wrong from inside a container — that's the container itself, not the host
running llama.cpp; (2) the runtime image has neither the `docker` CLI/compose
plugin nor (3) `git` installed — a bare JDK image ships neither, so
`ContainerOps`/`CodeWorkflow` are dead on arrival as specced; (4) Compose
derives its project name from the compose-file's directory, so the agent
running `docker compose up` against `/repo/docker-compose.yml` gets project
name `repo` while the real host stack is project `kafkamessagepersistencetomongodb`
— without pinning this, `buildAndRestart` would spin up a **second parallel
stack** instead of touching the running one; (5) `docker logs <name>` needs
real, project-prefixed container names (`kafkamessagepersistencetomongodb-consumer1-1`),
not bare service names; (6) every fresh container re-downloads the Gradle
distribution + all Maven Central deps on first `./gradlew test` unless a
named volume caches `~/.gradle`; (7) checking out a proposal branch in the
single bind-mounted working tree would switch the human's own live checkout
out from under them mid-review — needs `git worktree`, not a shared tree.

## Bootstrap (Branch 1, done directly — scaffolding exception)

- New `sre-agent-service/` directory mirroring `dashboard-service`'s structure:
  `build.gradle` (Spring Boot 3.5.7, Java 21 toolchain, spring-boot-starter-web +
  actuator + micrometer-registry-prometheus), `settings.gradle`
  (`rootProject.name = 'sre-agent-service'`), gradle wrapper files copied from
  an existing service, package `com.codrite.claimcheck.sreagent`.
- **Dockerfile (corrected, round 1):** two-stage as the other three services,
  but the runtime stage is `eclipse-temurin:21-jdk` (not `-jre` — the
  container needs a live JDK to run `./gradlew test` against the mounted repo
  as a subprocess, on top of running its own compiled bootJar as the web app;
  these are two genuinely different needs on the same JDK, not a conflation
  to resolve away). On top of the JDK base, the Dockerfile must additionally
  install: **`git`** (apt, Ubuntu-based temurin image), **the `docker` CLI +
  `docker-compose-plugin`** (apt via Docker's official apt repo, or static
  binaries — `docker.sock` only gives an API endpoint, not a client), and run
  `git config --global --add safe.directory '*'` at image-build time (the
  bind-mounted repo's UID won't match the container user, and git refuses to
  operate on a "dubious ownership" directory otherwise — this breaks the
  *first* `git status` without it).
- **LLM address (fixed, round 1):** `host.docker.internal:8080`, not
  `127.0.0.1:8080` — the local LLM runs on the host, not in this container.
  docker-compose.yml's `sre-agent-service` entry needs
  `extra_hosts: ["host.docker.internal:host-gateway"]` for Linux-host
  portability (Docker Desktop on Mac already resolves this name without it,
  but the explicit entry costs nothing and fixes Linux).
- docker-compose.yml: new `sre-agent-service` entry — build context
  `./sre-agent-service`, port `8086:8086`, volumes: `.:/repo` (repo root,
  read-write) and `/var/run/docker.sock:/var/run/docker.sock`, plus a named
  volume `sre-agent-gradle-cache:/root/.gradle` (round-1 fix 6, avoids a
  multi-minute cold `./gradlew test` on every fresh container). Depends on
  nothing (watches already-running services; reports no data if they're down).
- **Compose project-name pinning (round-1 fix 4):** any `docker compose`
  command the agent issues against `/repo/docker-compose.yml` MUST pass
  `-p kafkamessagepersistencetomongodb` (or set `COMPOSE_PROJECT_NAME`),
  matching the host stack's actual project name (derived from this repo's
  directory name), or `ContainerOps.buildAndRestart` silently stands up a
  second, parallel stack instead of touching the running one. `ContainerOps`
  must also explicitly refuse to target `sre-agent-service` itself (it should
  never rebuild/restart its own container mid-analysis).
- **Container name resolution (round-1 fix 5):** `LogSource` resolves the
  real, project-prefixed container name (e.g.
  `kafkamessagepersistencetomongodb-consumer1-1`) via `docker compose -p
  kafkamessagepersistencetomongodb ps --format ...`, rather than assuming the
  bare service name works directly with `docker logs`.
- Add `logging: driver: json-file, options: {max-size: "10m", max-file: "3"}`
  to producer/consumer1-3/dashboard service entries (log rotation, per spec).
- Data types (records, no logic): `MetricSnapshot`, `Finding`, `FindingStatus`
  (sealed: Observed/Confirmed/Proposed/Approved/Rejected), `IntervalState`,
  `FindingsHistory`. These live in `sre-agent-service/src/main/java/.../` and
  are the vocabulary the pure engine (Branch 2) operates on.

## Core seam (Branch 2): AnalysisEngine — pure, TDD

`AnalysisEngine.process(MetricSnapshot current, FindingsHistory history) ->
AnalysisResult` where `AnalysisResult(FindingsHistory updatedHistory, Duration
nextInterval, List<Finding> newlyGraduated)`.

Behavior:
- Compare `current` against each tracked metric's rolling baseline (stored in
  `history`) to detect "notably slower than its own baseline" — needs a
  concrete threshold, e.g. current value > 1.5x the trailing average of the
  last N snapshots for that metric+tag combination.
- Timing findings: increment a consecutive-cycle counter per metric+tag when
  above threshold; reset to 0 when back under threshold. Graduate
  (Observed -> Proposed) at 3 consecutive cycles.
- Quality findings: a separate input (pre-computed list of java-clean-code
  rule violations, since AnalysisEngine itself doesn't run the LLM or static
  analysis — that's the caller's job via ports) graduate immediately, first
  cycle they appear, IF not already present with status Rejected.
- Interval: start 2 min; if `newlyGraduated` is empty this cycle, double the
  previous interval (cap 10 min); if non-empty, reset to 2 min.
- Metric-to-class map: a separate small pure lookup,
  `MetricClassMap.classesFor(String metricName, Map<String,String> tags) ->
  List<String>` (returns fully-qualified class names or relative file paths),
  config-driven (a small YAML/properties resource per service), consulted
  when building a `Finding`'s file list — not part of AnalysisEngine's own
  state machine, a separate pure function it calls.
- Self-exclusion: AnalysisEngine (or the caller feeding it snapshots) never
  includes `sre-agent-service`'s own metrics/logs in scope.

## Ports (Branches 3-6): thin I/O boundaries, mirror PayloadReader/PayloadStore shape

- `MetricsSource.fetch(String serviceTarget) -> MetricSnapshot` — HTTP GET
  `/actuator/prometheus`, reuse `PrometheusScraper`-equivalent parsing (may
  literally port dashboard-service's `PrometheusScraper`/`MetricSample`
  pattern, duplicated per this codebase's existing no-shared-module
  convention).
- `LogSource.recentErrors(String containerName, Duration window) ->
  List<String>` — shells out to `docker logs --since <window> <container>`,
  filters to ERROR/stack-trace lines.
- `LlmClient.analyze(Finding candidate, List<SourceFile> mappedFiles,
  List<String> supportingLogs) -> Explanation` and
  `LlmClient.proposeDiff(Finding confirmed, List<SourceFile> mappedFiles) ->
  Diff` — two operations, both call `host.docker.internal:8080` (round-1 fix;
  NOT `127.0.0.1`, which is this container itself, not the host running
  llama.cpp).
- `CodeWorkflow.branchAndCommit(Diff diff) -> WorktreeRef`,
  `CodeWorkflow.runTests(WorktreeRef) -> TestResult`,
  `CodeWorkflow.mergeToMaster(WorktreeRef) -> void` — shells out to `git`
  (installed + `safe.directory` configured per the Dockerfile fix above) and
  `./gradlew test` against the mounted repo. **Round-1 fix 7:** branch/commit
  and test execution happen in a dedicated `git worktree` (e.g. under
  `/repo/.sre-agent/worktrees/<finding-id>/`), never by checking out the
  proposal branch in the shared `/repo` working tree — that would switch the
  human's own live checkout out from under them mid-review, and a dirty tree
  would block the checkout entirely. Only `mergeToMaster` touches the shared
  tree's `master` branch (a merge, not a checkout-and-edit), and the worktree
  is removed after merge or rejection.
- `ContainerOps.buildAndRestart(String service) -> void` — shells out to
  `docker compose -p kafkamessagepersistencetomongodb build <service>` then
  `docker compose -p kafkamessagepersistencetomongodb up -d --no-deps
  <service and replicas>` via the mounted socket (round-1 fix 4: the explicit
  `-p` pin is mandatory, or this spins up a second parallel stack under
  Compose's default directory-derived project name instead of touching the
  real one). Refuses to target `sre-agent-service` itself.

## Wiring + persistence (Branch 7)

- A scheduler component (mirrors `ScrapeScheduler`'s `@Scheduled` pattern, but
  with a mutable/adaptive rate rather than a fixed one — Spring's
  `@Scheduled(fixedRate=...)` doesn't support runtime-adjustable rates
  cleanly, so this needs a self-rescheduling `TaskScheduler.schedule(task,
  Instant)` loop instead, re-scheduling itself after each run using
  `AnalysisResult.nextInterval()`).
- `FindingsHistory` serialized to a JSON file on a mounted path (e.g.
  `/repo/.sre-agent/findings.json`) after every cycle; loaded on startup.

## Dashboard (Branch 8)

- New static page (own port 8086) — mirrors `dashboard-service`'s
  vanilla-JS + SSE/poll pattern: list of findings (status, first-observed
  cycle, confirmation count, explanation, diff once proposed), Approve/Reject
  buttons per finding, wired to new REST endpoints
  (`POST /api/findings/{id}/approve`, `POST /api/findings/{id}/reject`).

## Testing

- `AnalysisEngine`: TDD, RED-first, exactly like `StageProcessorTest` —
  baseline-threshold detection, 3-cycle confirmation, immediate quality-finding
  graduation, interval backoff/reset, no re-surfacing rejected findings.
- `MetricClassMap`: direct test per known metric+tag -> expected file list.
- Each port (`MetricsSource`, `LogSource`, `LlmClient`, `CodeWorkflow`,
  `ContainerOps`): tested via fakes at the call site that uses them (the
  scheduler component), not by hitting real Docker/git/LLM in unit tests.
- Verify gate: `./gradlew test` in `sre-agent-service`, matching the other
  three services' convention.

## Sequencing (keep every intermediate state green)

1. Bootstrap (Branch 1): scaffold + data types, compose entry, log rotation.
   Verify: `docker compose config` validates, new service's empty test suite
   passes (no tests yet, just confirms the Gradle setup builds).
2. AnalysisEngine + MetricClassMap (Branch 2): TDD, the pure core.
3. MetricsSource + LogSource (Branch 3).
4. LlmClient (Branch 4).
5. CodeWorkflow (Branch 5).
6. ContainerOps (Branch 6).
7. Scheduler wiring + persistence (Branch 7) — first point the whole loop
   runs end to end, even with a minimal/no UI.
8. Dashboard UI (Branch 8).

Given the scope, branches 3-8 may span multiple sessions; only branches 1-2
are committed to for this pass unless time allows more.

## Risks / open items for review

- The "1.5x rolling baseline" timing threshold is a placeholder — needs
  validation against real metric noise (the relay chain's own p95/p99 already
  show natural variance per the live run this session; a threshold too tight
  will false-positive on normal jitter).
- `CodeWorkflow.runTests` running a full service's Gradle test suite inside
  the sre-agent container against a live bind-mounted repo risks interfering
  with a human's own concurrent edits to the same working tree — worth
  flagging that this agent and a human should not edit the repo simultaneously
  during an active analysis-to-merge cycle.
- Self-rescheduling with `TaskScheduler`: overlap is avoided by construction
  as specced — the next run is scheduled from `AnalysisResult.nextInterval()`
  computed at the *end* of the current cycle, not scheduled up-front at a
  fixed cadence, so a slow cycle simply pushes its own next run out rather
  than overlapping. State this explicitly in the scheduler's implementation
  (Branch 7) so it isn't accidentally "fixed" into a fixed-rate scheduler
  later.
