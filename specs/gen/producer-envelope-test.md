Write a JUnit 5 test class.

Output path: producer-service/src/test/java/com/codrite/claimcheck/producer/MessageEnvelopeTest.java
Package: com.codrite.claimcheck.producer

The following types DO NOT EXIST YET. Reference them exactly as declared below;
do NOT declare them in your output. Your output is EXACTLY ONE top-level class:
MessageEnvelopeTest. Exactly ONE fenced code block, nothing outside it. Max 120 lines.

```java
public enum DeliveryPath { INLINE, CLAIM_CHECK }

@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
public record MessageEnvelope(
    String messageId, String pairId, java.time.Instant createdAt,
    long producedAtEpochNanos, long payloadSizeBytes,
    DeliveryPath forcedPath,   // nullable
    String payload,            // nullable; exactly one of payload/mongoId is set
    String mongoId) {}         // nullable
```

Use a single ObjectMapper field constructed as:
`new ObjectMapper().registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())`

Tests required (plain JUnit 5, org.assertj.core.api.Assertions.assertThat):
1. jsonRoundTripInlineEnvelope: build an envelope with payload "hello",
   mongoId null, forcedPath DeliveryPath.INLINE, createdAt Instant.parse("2026-07-15T10:00:00Z"),
   producedAtEpochNanos 123456789L, payloadSizeBytes 5. Serialize with
   writeValueAsString, deserialize with readValue, assert the result equals the original record.
2. jsonRoundTripClaimCheckEnvelope: payload null, mongoId "665f1a2b3c4d5e6f77889900",
   forcedPath null; round-trip and assert equality.
3. inlineJsonOmitsNullFields: serialize the envelope from test 1 and assert the
   JSON string does not contain "mongoId".

writeValueAsString/readValue throw JsonProcessingException: declare
`throws Exception` on each test method. No Mockito. No @SpringBootTest.

Construct each MessageEnvelope with literal arguments passed directly to the
constructor — do NOT introduce local variables for the arguments, and NEVER
write `var x = null` (Java cannot infer a type from null; it does not compile).
Write null arguments inline as `null` in the constructor call.
