package com.codrite.claimcheck.dashboard;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.Map;

class PrometheusScraperTest {

    private static final String BODY = """
            # HELP consumer_messages_total Messages consumed
            # TYPE consumer_messages_total counter
            consumer_messages_total{path="INLINE"} 42.0
            consumer_messages_total{path="CLAIM_CHECK"} 40.0
            consumer_e2e_latency_seconds{path="INLINE",quantile="0.5"} 0.011
            consumer_e2e_latency_seconds{path="INLINE",quantile="0.99"} NaN
            consumer_e2e_latency_seconds_count{path="INLINE"} 42.0
            consumer_e2e_latency_seconds_sum{path="INLINE"} 0.6
            jvm_threads_live_threads 23.0""";

    private static MetricSample find(List<MetricSample> samples, String name, String tagKey, String tagValue) {
        for (MetricSample s : samples) {
            if (s.name().equals(name)) {
                if (tagKey == null) {
                    if (s.tags().isEmpty()) return s;
                } else if (tagValue != null && tagValue.equals(s.tags().get(tagKey))) {
                    return s;
                }
            }
        }
        return null;
    }

    @Test
    void parsesAllNonCommentNonNanLines() {
        List<MetricSample> samples = PrometheusScraper.parse(BODY);
        assertEquals(6, samples.size());
    }

    @Test
    void parsesCounterWithTags() {
        List<MetricSample> samples = PrometheusScraper.parse(BODY);
        MetricSample s = find(samples, "consumer_messages_total", "path", "INLINE");
        assertNotNull(s);
        assertEquals(42.0, s.value(), 1e-9);
        assertEquals("INLINE", s.tags().get("path"));
    }

    @Test
    void parsesQuantileTag() {
        List<MetricSample> samples = PrometheusScraper.parse(BODY);
        MetricSample s = find(samples, "consumer_e2e_latency_seconds", "quantile", "0.5");
        assertNotNull(s);
        assertEquals(0.011, s.value(), 1e-9);
        assertEquals("INLINE", s.tags().get("path"));
    }

    @Test
    void skipsNaN() {
        List<MetricSample> samples = PrometheusScraper.parse(BODY);
        MetricSample s = find(samples, "consumer_e2e_latency_seconds", "quantile", "0.99");
        assertNull(s);
    }

    @Test
    void parsesSumCountSuffixLines() {
        List<MetricSample> samples = PrometheusScraper.parse(BODY);
        MetricSample count = find(samples, "consumer_e2e_latency_seconds_count", "path", "INLINE");
        assertEquals(42.0, count.value(), 1e-9);
        MetricSample sum = find(samples, "consumer_e2e_latency_seconds_sum", "path", "INLINE");
        assertEquals(0.6, sum.value(), 1e-9);
    }

    @Test
    void parsesUntaggedSample() {
        List<MetricSample> samples = PrometheusScraper.parse(BODY);
        MetricSample s = find(samples, "jvm_threads_live_threads", null, null);
        assertNotNull(s);
        assertEquals(23.0, s.value(), 1e-9);
        assertTrue(s.tags().isEmpty());
    }
}
