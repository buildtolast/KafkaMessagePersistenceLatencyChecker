Write a JUnit 5 test class (plain unit test, NO Spring).

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: dashboard-service/src/test/java/com/codrite/claimcheck/dashboard/PrometheusScraperTest.java
Package: com.codrite.claimcheck.dashboard

These types are in the SAME package — reference them, do NOT declare them, do NOT
import them:
- record MetricSample(String name, java.util.Map<String,String> tags, double value)
- final class PrometheusScraper with ONE static method:
  `public static java.util.List<MetricSample> parse(String body)`

Output EXACTLY ONE top-level class PrometheusScraperTest, max 90 lines.
Imports: org.junit.jupiter.api.Test, static org.junit.jupiter.api.Assertions.*,
java.util.List, java.util.Map. NO other imports, NO AssertJ, NO Mockito.

Use this EXACT sample as a private static final String BODY, built with a Java
text block ("""), copied verbatim including the comment lines:

# HELP consumer_messages_total Messages consumed
# TYPE consumer_messages_total counter
consumer_messages_total{path="INLINE"} 42.0
consumer_messages_total{path="CLAIM_CHECK"} 40.0
consumer_e2e_latency_seconds{path="INLINE",quantile="0.5"} 0.011
consumer_e2e_latency_seconds{path="INLINE",quantile="0.99"} NaN
consumer_e2e_latency_seconds_count{path="INLINE"} 42.0
consumer_e2e_latency_seconds_sum{path="INLINE"} 0.6
jvm_threads_live_threads 23.0

Helper: a private static method
`MetricSample find(List<MetricSample> samples, String name, String tagKey, String tagValue)`
that loops (plain for loop, no streams) and returns the first sample whose name
equals `name` and whose tags map contains the given key/value (when tagKey is null,
match samples with an EMPTY tags map); return null if not found.

Tests (call PrometheusScraper.parse(BODY) in each):
1. parsesAllNonCommentNonNanLines: assertEquals(6, samples.size()) — 7 data lines
   minus the NaN line; comment lines ignored.
2. parsesCounterWithTags: find "consumer_messages_total" with path=INLINE →
   assertNotNull, assertEquals(42.0, value, 1e-9), assertEquals("INLINE", tags.get("path")).
3. parsesQuantileTag: find "consumer_e2e_latency_seconds" with quantile=0.5 →
   assertNotNull, assertEquals(0.011, value, 1e-9), assertEquals("INLINE", tags.get("path")).
4. skipsNaN: find "consumer_e2e_latency_seconds" with quantile=0.99 → assertNull.
5. parsesSumCountSuffixLines: find "consumer_e2e_latency_seconds_count" with
   path=INLINE → assertEquals(42.0, value, 1e-9); find
   "consumer_e2e_latency_seconds_sum" with path=INLINE → assertEquals(0.6, value, 1e-9).
6. parsesUntaggedSample: find "jvm_threads_live_threads" with tagKey null →
   assertNotNull, assertEquals(23.0, value, 1e-9), assertTrue(tags.isEmpty()).
