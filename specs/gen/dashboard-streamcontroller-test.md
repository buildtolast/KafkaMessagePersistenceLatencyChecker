Write a Spring Boot integration test for the dashboard scrape/stream slice.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: dashboard-service/src/test/java/com/codrite/claimcheck/dashboard/StreamControllerTest.java
Package: com.codrite.claimcheck.dashboard

ALREADY EXIST in this package (reference, do NOT declare any of them):
- record ComparisonModel(Map<String,PathStats> byPath, List<InstanceHealth> instances, long mongoDocs, double mongoBytes)
- class ScrapeScheduler with methods: `public void scrape()` and `public ComparisonModel latest()`
- class StreamController exposing GET /api/stats and GET /api/stream (SSE)

Output EXACTLY ONE top-level class `StreamControllerTest`, max 110 lines.

Imports (use EXACTLY these, no wildcard imports, no others):
```
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
```

Class annotations:
`@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)`

Static fields: two `MockWebServer` instances (server1, server2).

The Prometheus body BOTH servers return for every request (a private static final String,
lines joined with "\n"):
```
consumer_messages_total{path="INLINE"} 100.0
consumer_e2e_latency_seconds{path="INLINE",quantile="0.5"} 0.010
consumer_e2e_latency_seconds_count{path="INLINE"} 10.0
producer_messages_total{path="CLAIM_CHECK"} 7.0
producer_bytes_total{path="CLAIM_CHECK"} 14000000.0
```
Use a Dispatcher whose dispatch(RecordedRequest) always returns
`new MockResponse().setResponseCode(200).setBody(BODY)`.

@BeforeAll static setup: create + start both servers, set the dispatcher on both.
@AfterAll static teardown: shutdown both (method declares `throws IOException`).

@DynamicPropertySource static method: register `app.dashboard.targets` as
server1 base url + "," + server2 base url (strip trailing '/' from
`server.url("/").toString()` before joining).

Injected: `@LocalServerPort int port;` `@Autowired TestRestTemplate rest;`
`@Autowired ScrapeScheduler scheduler;`

Test 1 `statsEndpointReturnsAggregatedModel`:
- call `scheduler.scrape();`
- `ResponseEntity<ComparisonModel> resp = rest.getForEntity("/api/stats", ComparisonModel.class);`
- assert status 200; body not null; `body.byPath()` contains key "INLINE";
  `assertEquals(10.0, body.byPath().get("INLINE").e2eP50Ms(), 1e-6);`
  `assertEquals(14L, body.mongoDocs());` (7 from each of the two targets)

Test 2 `streamEndpointEmitsAnSseEvent` (declares `throws Exception`):
- call `scheduler.scrape();`
- open `http://localhost:` + port + `/api/stream` with java.net.http.HttpClient,
  `HttpResponse.BodyHandlers.ofInputStream()`, request timeout Duration.ofSeconds(10).
- wrap the input stream in a BufferedReader (StandardCharsets.UTF_8); read lines
  in a loop (max 50 lines) until one starts with "data:"; assert such a line was
  found and that it contains "byPath".

No Thread.sleep. No awaitility. No mocking frameworks.
