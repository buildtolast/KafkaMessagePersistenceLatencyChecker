Write a JUnit 5 test class.

Output path: producer-service/src/test/java/com/codrite/claimcheck/producer/ClaimCheckRouterTest.java
Package: com.codrite.claimcheck.producer

These types ALREADY EXIST in package com.codrite.claimcheck.producer — reference
them, do NOT declare them:

```java
public enum DeliveryPath { INLINE, CLAIM_CHECK }
public record MessageEnvelope(String messageId, String pairId, java.time.Instant createdAt,
    long producedAtEpochNanos, long payloadSizeBytes, DeliveryPath forcedPath,
    String payload, String mongoId) {}
```

These types DO NOT EXIST YET — reference them as declared here, do NOT declare them:

```java
public interface PayloadStore { String store(String payload, long sizeBytes); }

public final class ClaimCheckRouter {
  public ClaimCheckRouter(long thresholdBytes, PayloadStore store) {}
  public DeliveryPath decide(long payloadSizeBytes, DeliveryPath forcedPath) {}
  public MessageEnvelope route(String messageId, String pairId, String payload, DeliveryPath forcedPath) {}
}
```

Semantics under test: decide() returns forcedPath when non-null, else CLAIM_CHECK
iff payloadSizeBytes > thresholdBytes. route() measures size as
payload.getBytes(UTF_8).length; on CLAIM_CHECK it calls store.store(payload, size)
and builds an envelope with mongoId set and payload null; on INLINE payload set,
mongoId null, store NOT called. route() fills messageId/pairId/forcedPath/size.

Your output is EXACTLY ONE top-level class ClaimCheckRouterTest with a nested
static class RecordingStore implements PayloadStore (records last payload/size,
counts calls, returns "abc123"). No Mockito. Exactly ONE fenced code block,
max 140 lines. Use a router built with threshold 2097152L. Constants: build a
String of exactly 2097152 chars via "x".repeat(2097152) (1 char = 1 UTF-8 byte).

Tests required (org.assertj assertions):
1. exactlyThresholdIsInline: decide(2097152L, null) == INLINE
2. oneByteOverThresholdIsClaimCheck: decide(2097153L, null) == CLAIM_CHECK
3. forcedInlineOverridesSizeAndSkipsStore: route("m","p", "x".repeat(2097153), DeliveryPath.INLINE)
   -> envelope.payload() non-null, mongoId() null, store call count 0.
   CRITICAL: the repeat count in this test is 2097153 (one byte OVER threshold,
   NOT 2097152) — the payload must be oversized so the test proves the forced
   INLINE flag overrides the size rule.
4. forcedClaimCheckStoresTinyPayload: route("m","p","hi", DeliveryPath.CLAIM_CHECK)
   -> mongoId() equals "abc123", payload() null, store received "hi" with size 2
5. inlineRouteLeavesStoreUntouched: route("m","p","hello", null)
   -> payload() "hello", mongoId() null, store call count 0, payloadSizeBytes() 5,
      messageId() "m", pairId() "p"
