Write a Java class.
Output path: producer-service/src/main/java/com/codrite/claimcheck/producer/ProducerMetrics.java
Package: com.codrite.claimcheck.producer
DeliveryPath ALREADY EXISTS: public enum DeliveryPath { INLINE, CLAIM_CHECK } — do NOT declare it.
Output EXACTLY ONE top-level class, ONE fenced code block, max 45 lines.

@org.springframework.stereotype.Component
public final class ProducerMetrics {
  public ProducerMetrics(io.micrometer.core.instrument.MeterRegistry registry)
  public io.micrometer.core.instrument.Timer mongoInsert(DeliveryPath p)
  public io.micrometer.core.instrument.Timer kafkaSend(DeliveryPath p)
  public void recordMessage(DeliveryPath p, long bytes)
}

mongoInsert returns Timer.builder("producer.mongo.insert").tag("path", p.name()).publishPercentileHistogram().register(registry) — build on each call is fine (micrometer dedupes).
kafkaSend same with name "producer.kafka.send".
recordMessage increments Counter "producer.messages" tag path=p.name() by 1 and Counter "producer.bytes" same tag by bytes (use registry.counter(name, "path", p.name()).increment(...)).
Store the registry in a field. No other methods.
