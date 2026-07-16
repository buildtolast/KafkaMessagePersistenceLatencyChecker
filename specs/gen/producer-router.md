Write a Java class.

Output path: producer-service/src/main/java/com/codrite/claimcheck/producer/ClaimCheckRouter.java
Package: com.codrite.claimcheck.producer

These types ALREADY EXIST in this package — reference them, do NOT declare them:

```java
public enum DeliveryPath { INLINE, CLAIM_CHECK }
public record MessageEnvelope(String messageId, String pairId, java.time.Instant createdAt,
    long producedAtEpochNanos, long payloadSizeBytes, DeliveryPath forcedPath,
    String payload, String mongoId) {}
public interface PayloadStore { String store(String payload, long sizeBytes); }
```

Output EXACTLY ONE top-level class. Exactly ONE fenced code block, max 60 lines.

```java
public final class ClaimCheckRouter {
  public ClaimCheckRouter(long thresholdBytes, PayloadStore store)
  public DeliveryPath decide(long payloadSizeBytes, DeliveryPath forcedPath)
  public MessageEnvelope route(String messageId, String pairId, String payload, DeliveryPath forcedPath)
}
```

decide: return forcedPath if non-null; else payloadSizeBytes > thresholdBytes
? CLAIM_CHECK : INLINE.

route: size = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
path = decide(size, forcedPath); createdAt = java.time.Instant.now();
producedAtEpochNanos = createdAt.getEpochSecond() * 1_000_000_000L + createdAt.getNano().
If path == CLAIM_CHECK: mongoId = store.store(payload, size), envelope payload null.
Else envelope payload = payload, mongoId null. forcedPath passes through unchanged
into the envelope. No logging, no validation, no other methods.
