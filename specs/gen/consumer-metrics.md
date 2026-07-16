Write a Spring @Component class.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: consumer-service/src/main/java/com/codrite/claimcheck/consumer/ConsumerMetrics.java
Package: com.codrite.claimcheck.consumer

ALREADY EXISTS in this package (reference, do NOT declare): enum DeliveryPath { INLINE, CLAIM_CHECK }.

Output EXACTLY ONE top-level class ConsumerMetrics, max 60 lines. Model it on this
producer sibling (same style, io.micrometer.core.instrument.{MeterRegistry,Timer,Counter}):

- Constructor: `public ConsumerMetrics(MeterRegistry registry)` storing the registry
  in a private final field.
- `public Timer e2eLatency(DeliveryPath p)` → Timer.builder("consumer.e2e.latency")
  .tag("path", p.name()).publishPercentileHistogram().register(registry)
- `public Timer processing(DeliveryPath p)` → same pattern, name "consumer.processing"
- `public Timer mongoFetch()` → same pattern, name "consumer.mongo.fetch",
  tag("path", DeliveryPath.CLAIM_CHECK.name())
- `public void recordE2e(DeliveryPath p, java.time.Duration d)` → e2eLatency(p).record(d)
- `public void recordProcessing(DeliveryPath p, java.time.Duration d)` → processing(p).record(d)
- `public void recordMessage(DeliveryPath p, long bytes)` →
  registry.counter("consumer.messages", "path", p.name()).increment();
  registry.counter("consumer.bytes", "path", p.name()).increment(bytes);

Annotate the class @org.springframework.stereotype.Component. NO wildcard imports.
