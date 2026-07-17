Write the dashboard REST + SSE controller.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: dashboard-service/src/main/java/com/codrite/claimcheck/dashboard/StreamController.java
Package: com.codrite.claimcheck.dashboard

ALREADY EXIST in this package (reference, do NOT declare any of them):
- class ScrapeScheduler with `public ComparisonModel latest()`
- record ComparisonModel(...)

Output EXACTLY ONE top-level class, max 60 lines:

@RestController
public class StreamController

Imports (no wildcard imports): org.springframework.web.bind.annotation.GetMapping,
org.springframework.web.bind.annotation.RestController,
org.springframework.web.servlet.mvc.method.annotation.SseEmitter,
org.springframework.scheduling.annotation.Scheduled,
java.io.IOException, java.util.concurrent.CopyOnWriteArrayList.

Fields:
- `private final ScrapeScheduler scheduler;` (constructor injection)
- `private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();`

`@GetMapping("/api/stats")`
`public ComparisonModel stats()` — return `scheduler.latest();`

`@GetMapping("/api/stream")`
`public SseEmitter stream()`:
- `SseEmitter emitter = new SseEmitter(0L);`
- register removal: onCompletion, onTimeout, onError each remove the emitter
  from the list.
- IMMEDIATELY send the current model once:
  `emitter.send(SseEmitter.event().name("stats").data(scheduler.latest()));`
  wrapped in try/catch (IOException | IllegalStateException) → on failure
  `emitter.completeWithError(e)` — but STILL add to list only on success.
- add to `emitters`, return it.

`@Scheduled(fixedRate = 2000)`
`public void push()`:
- snapshot `scheduler.latest()` once; for each emitter, try to send the same
  event (name "stats"); catch (IOException | IllegalStateException) → remove
  that emitter and `emitter.completeWithError(e)`.
