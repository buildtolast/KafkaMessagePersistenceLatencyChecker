Write the dashboard scrape scheduler component.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: dashboard-service/src/main/java/com/codrite/claimcheck/dashboard/ScrapeScheduler.java
Package: com.codrite.claimcheck.dashboard

ALREADY EXIST in this package (reference, do NOT declare any of them):
- record MetricSample(String name, Map<String,String> tags, double value)
- final class PrometheusScraper with `public static List<MetricSample> parse(String body)`
- final class MetricsAggregator with constructor `MetricsAggregator(double windowSeconds)`
  and `public synchronized ComparisonModel aggregate(Map<String, List<MetricSample>> snapshots)`
- record ComparisonModel(Map<String,PathStats> byPath, List<InstanceHealth> instances, long mongoDocs, double mongoBytes)

Output EXACTLY ONE top-level class, max 80 lines:

@Component
public class ScrapeScheduler

Imports (no wildcard imports): org.springframework.stereotype.Component,
org.springframework.beans.factory.annotation.Value,
org.springframework.scheduling.annotation.Scheduled,
java.net.URI, java.net.http.HttpClient, java.net.http.HttpRequest,
java.net.http.HttpResponse, java.time.Duration, java.util.HashMap, java.util.List,
java.util.Map, java.util.concurrent.atomic.AtomicReference.

Fields:
- `private final List<String> targets;`
- `private final MetricsAggregator aggregator = new MetricsAggregator(2.0);`
- `private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();`
- `private final AtomicReference<ComparisonModel> latest = new AtomicReference<>(new ComparisonModel(Map.of(), List.of(), 0L, 0.0));`

Constructor: `public ScrapeScheduler(@Value("${app.dashboard.targets}") String targetsCsv)` —
split on ',', trim each entry, skip blanks, store as List; if the resulting list
is empty, `throw new IllegalStateException("app.dashboard.targets is empty");`.

`@Scheduled(fixedRate = 2000)`
`public void scrape()`:
- build `Map<String, List<MetricSample>> snapshots = new HashMap<>();`
- for each target: GET target + "/actuator/prometheus" with a 1500 ms request
  timeout (`HttpRequest.newBuilder(...).timeout(...)`, BodyHandlers.ofString()).
  On status 200, put `snapshots.put(target, PrometheusScraper.parse(body))`.
  On failure or non-200: leave that target out of the map (that marks the
  instance down) — never rethrow. Two catch blocks per target, in this order:
  1. `catch (InterruptedException e)` → `Thread.currentThread().interrupt(); return;`
     (MUST restore the interrupt flag and abort the whole scrape pass).
  2. `catch (Exception e)` → `log.debug("scrape failed for {}: {}", target, e.toString());`
- Add a logger field: `private static final Logger log = LoggerFactory.getLogger(ScrapeScheduler.class);`
  with imports org.slf4j.Logger and org.slf4j.LoggerFactory.
- `latest.set(aggregator.aggregate(snapshots));`

`public ComparisonModel latest()` — return `latest.get();`
