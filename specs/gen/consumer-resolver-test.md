Write a JUnit 5 test class.

Output path: consumer-service/src/test/java/com/codrite/claimcheck/consumer/ClaimCheckResolverTest.java
Package: com.codrite.claimcheck.consumer

These types ALREADY EXIST in package com.codrite.claimcheck.consumer — reference
them, do NOT declare them. PayloadReader, ResolvedMessage, and ClaimCheckResolver
DO NOT EXIST YET — reference them as if they exist, do NOT declare them:

```java
public enum DeliveryPath { INLINE, CLAIM_CHECK }
public record MessageEnvelope(String messageId, String pairId, java.time.Instant createdAt,
    long producedAtEpochNanos, long payloadSizeBytes, DeliveryPath forcedPath,
    String payload, String mongoId) {}
public interface PayloadReader { java.util.Optional<String> fetch(String mongoId); }
public record ResolvedMessage(String messageId, DeliveryPath path, String payload, long sizeBytes) {}
public final class ClaimCheckResolver {
  public ClaimCheckResolver(PayloadReader reader) {}
  /** empty Optional when a claim-check document is missing (caller logs+skips). */
  public java.util.Optional<ResolvedMessage> resolve(MessageEnvelope env) {}
}
```

Do NOT use Mockito. Use a small recording fake implementing PayloadReader as a
nested static class inside the test: it stores a Map<String,String> of documents,
records every mongoId passed to fetch in a List<String>, and returns
Optional.ofNullable(map.get(mongoId)).

Tests (JUnit 5, plain org.junit.jupiter.api.Assertions):

1. inlineEnvelopeResolvesWithoutReader: envelope with mongoId=null, payload="hello",
   payloadSizeBytes=5 → resolve returns present ResolvedMessage with
   path=DeliveryPath.INLINE, payload "hello", sizeBytes 5, messageId matching the
   envelope; the fake's recorded fetch list is empty.
2. claimCheckEnvelopeFetchesFromReader: envelope with mongoId="abc", payload=null,
   payloadSizeBytes=7; fake contains "abc" → "stored-payload" → resolve returns
   present ResolvedMessage with path=DeliveryPath.CLAIM_CHECK,
   payload "stored-payload", sizeBytes 7; fake recorded exactly one fetch of "abc".
3. missingDocumentReturnsEmpty: envelope with mongoId="missing", payload=null;
   fake has no documents → resolve returns Optional.empty(); fake recorded one
   fetch of "missing".

Build envelopes with `new MessageEnvelope(id, "pair", java.time.Instant.now(), 0L,
size, null, payload, mongoId)`.

Exactly ONE fenced code block, exactly one top-level class, max 100 lines,
no commentary outside the fence.
