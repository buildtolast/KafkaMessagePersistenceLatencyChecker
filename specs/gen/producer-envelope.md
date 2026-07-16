Write a Java record.

Output path: producer-service/src/main/java/com/codrite/claimcheck/producer/MessageEnvelope.java
Package: com.codrite.claimcheck.producer

DeliveryPath ALREADY EXISTS in this package as `public enum DeliveryPath { INLINE, CLAIM_CHECK }`
— reference it, do NOT declare it. Output EXACTLY ONE top-level type: the record
MessageEnvelope. Exactly ONE fenced code block, nothing outside it. Max 30 lines.

Exact contract:

```java
@JsonInclude(JsonInclude.Include.NON_NULL)
public record MessageEnvelope(
    String messageId, String pairId, java.time.Instant createdAt,
    long producedAtEpochNanos, long payloadSizeBytes,
    DeliveryPath forcedPath,   // nullable
    String payload,            // nullable; exactly one of payload/mongoId is set
    String mongoId) {}         // nullable
```

Import com.fasterxml.jackson.annotation.JsonInclude. No compact constructor,
no validation, no extra methods. Records auto-generate equals/hashCode — add nothing.
