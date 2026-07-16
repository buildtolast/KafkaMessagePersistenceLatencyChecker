Write a Java class.
Output path: producer-service/src/main/java/com/codrite/claimcheck/producer/EnvelopePublisher.java
Package: com.codrite.claimcheck.producer
ALREADY EXIST (do NOT declare): DeliveryPath enum { INLINE, CLAIM_CHECK };
MessageEnvelope record (fields messageId, pairId, createdAt, producedAtEpochNanos,
payloadSizeBytes, forcedPath, payload, mongoId — accessor style env.pairId());
ProducerMetrics with Timer kafkaSend(DeliveryPath p) and void recordMessage(DeliveryPath p, long bytes).
Output EXACTLY ONE top-level class, ONE fenced code block, max 45 lines.

@org.springframework.stereotype.Component
public final class EnvelopePublisher {
  public EnvelopePublisher(org.springframework.kafka.core.KafkaTemplate<String,String> kafka,
                           com.fasterxml.jackson.databind.ObjectMapper mapper,
                           ProducerMetrics metrics)
  public void publish(MessageEnvelope env)
}

publish(): DeliveryPath path = env.mongoId() != null ? DeliveryPath.CLAIM_CHECK : DeliveryPath.INLINE;
String json = mapper.writeValueAsString(env) (wrap JsonProcessingException in new
IllegalStateException); Timer.Sample sample = Timer.start(); kafka.send("messages",
env.pairId(), json).join(); sample.stop(metrics.kafkaSend(path));
metrics.recordMessage(path, env.payloadSizeBytes()). Import
io.micrometer.core.instrument.Timer. No other methods.
