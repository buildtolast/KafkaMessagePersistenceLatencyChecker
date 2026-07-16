Write a Java class.
Output path: producer-service/src/main/java/com/codrite/claimcheck/producer/PairGenerator.java
Package: com.codrite.claimcheck.producer
ALREADY EXIST (do NOT declare): DeliveryPath enum { INLINE, CLAIM_CHECK };
MessageEnvelope record; ClaimCheckRouter with
MessageEnvelope route(String messageId, String pairId, String payload, DeliveryPath forcedPath);
EnvelopePublisher with void publish(MessageEnvelope env).
Output EXACTLY ONE top-level class, ONE fenced code block, max 45 lines.

CRITICAL: the TWO class-level annotations below MUST be present ON the class
declaration (a previous generation imported them but forgot to apply them,
which silently disabled the whole feature):
@Component
@ConditionalOnProperty(name = "app.load.enabled", havingValue = "true", matchIfMissing = true)
public final class PairGenerator { ... }
And the @Scheduled(fixedRateString = "${app.load.tick-millis:200}") annotation
MUST be on tick().

@org.springframework.stereotype.Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = "app.load.enabled", havingValue = "true", matchIfMissing = true)
public final class PairGenerator {
  public PairGenerator(ClaimCheckRouter router, EnvelopePublisher publisher,
      @org.springframework.beans.factory.annotation.Value("${app.load.payload-bytes:2097152}") int payloadBytes)
  @org.springframework.scheduling.annotation.Scheduled(fixedRateString = "${app.load.tick-millis:200}")
  public void tick()
}

tick(): String payload = "x".repeat(payloadBytes); String pairId =
java.util.UUID.randomUUID().toString(); publisher.publish(router.route(
UUID.randomUUID().toString(), pairId, payload, DeliveryPath.INLINE));
publisher.publish(router.route(UUID.randomUUID().toString(), pairId, payload,
DeliveryPath.CLAIM_CHECK)); Log one line per tick at info with
org.slf4j.Logger/LoggerFactory: pairId and payloadBytes. No other methods.
