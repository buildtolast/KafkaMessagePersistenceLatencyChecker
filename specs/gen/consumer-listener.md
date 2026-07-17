Write a Spring Kafka listener component.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: consumer-service/src/main/java/com/codrite/claimcheck/consumer/EnvelopeListener.java
Package: com.codrite.claimcheck.consumer

ALREADY EXIST in this package (reference, do NOT declare ANY of them):
- enum DeliveryPath { INLINE, CLAIM_CHECK }
- record MessageEnvelope(String messageId, String pairId, java.time.Instant createdAt,
    long producedAtEpochNanos, long payloadSizeBytes, DeliveryPath forcedPath,
    String payload, String mongoId)
- record ResolvedMessage(String messageId, DeliveryPath path, String payload, long sizeBytes)
- final class ClaimCheckResolver { public java.util.Optional<ResolvedMessage> resolve(MessageEnvelope env) {...} }
- class ConsumerMetrics { public void recordE2e(DeliveryPath p, java.time.Duration d) {}
    public void recordProcessing(DeliveryPath p, java.time.Duration d) {}
    public void recordMessage(DeliveryPath p, long bytes) {} }

Output EXACTLY ONE top-level class, max 70 lines:

@Component public final class EnvelopeListener
- private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(EnvelopeListener.class);
- Constructor: `public EnvelopeListener(ClaimCheckResolver resolver, ConsumerMetrics metrics, com.fasterxml.jackson.databind.ObjectMapper mapper)`
  storing all three in private final fields.
- One method:
  @org.springframework.kafka.annotation.KafkaListener(topics = "messages", groupId = "claim-check-demo")
  public void onMessage(String value)
  Body, in order:
  1. Deserialize: `MessageEnvelope env = mapper.readValue(value, MessageEnvelope.class);`
     mapper.readValue on a String throws the CHECKED exception
     com.fasterxml.jackson.core.JsonProcessingException — wrap ONLY this call in
     try/catch (JsonProcessingException e) { log.error("failed to deserialize envelope", e); return; }
     The method must NOT declare `throws`.
  2. `long startNanos = System.nanoTime();`
  3. `java.util.Optional<ResolvedMessage> resolved = resolver.resolve(env);`
  4. If empty: `log.error("missing claim-check document mongoId={} messageId={}", env.mongoId(), env.messageId()); return;`
     Do NOT throw.
  5. ResolvedMessage msg = resolved.get();
     java.time.Duration processing = java.time.Duration.ofNanos(System.nanoTime() - startNanos);
     java.time.Instant now = java.time.Instant.now();
     long nowEpochNanos = now.getEpochSecond() * 1_000_000_000L + now.getNano();
     java.time.Duration e2e = java.time.Duration.ofNanos(Math.max(0L, nowEpochNanos - env.producedAtEpochNanos()));
     (producedAtEpochNanos is EPOCH-based — never compare it against System.nanoTime(),
      whose origin is arbitrary. The Math.max clamp is REQUIRED: with clock skew the
      difference can go negative and Micrometer silently drops negative recordings.)
     metrics.recordProcessing(msg.path(), processing);
     metrics.recordE2e(msg.path(), e2e);
     metrics.recordMessage(msg.path(), msg.sizeBytes());
  6. log.info("consumed messageId={} path={} sizeBytes={} e2eMs={}",
       msg.messageId(), msg.path(), msg.sizeBytes(), e2e.toMillis());

NO wildcard imports. Do not add any other methods or fields.
