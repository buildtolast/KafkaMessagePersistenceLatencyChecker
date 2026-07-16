Write a Java class.

Output path: consumer-service/src/main/java/com/codrite/claimcheck/consumer/ClaimCheckResolver.java
Package: com.codrite.claimcheck.consumer

These types ALREADY EXIST in package com.codrite.claimcheck.consumer — reference
them, do NOT declare them:

```java
public enum DeliveryPath { INLINE, CLAIM_CHECK }
public record MessageEnvelope(String messageId, String pairId, java.time.Instant createdAt,
    long producedAtEpochNanos, long payloadSizeBytes, DeliveryPath forcedPath,
    String payload, String mongoId) {}
public interface PayloadReader { java.util.Optional<String> fetch(String mongoId); }
public record ResolvedMessage(String messageId, DeliveryPath path, String payload, long sizeBytes) {}
```

Implement:

```java
public final class ClaimCheckResolver {
  public ClaimCheckResolver(PayloadReader reader) {}
  public java.util.Optional<ResolvedMessage> resolve(MessageEnvelope env) {}
}
```

Semantics: if env.mongoId() is null the envelope is INLINE — return
Optional.of(new ResolvedMessage(env.messageId(), DeliveryPath.INLINE,
env.payload(), env.payloadSizeBytes())). Otherwise it is CLAIM_CHECK — call
reader.fetch(env.mongoId()); if empty return Optional.empty(); else return a
ResolvedMessage with path CLAIM_CHECK, the fetched payload, and
env.payloadSizeBytes().

Exactly ONE fenced code block, exactly one top-level class, max 40 lines,
no commentary outside the fence.
