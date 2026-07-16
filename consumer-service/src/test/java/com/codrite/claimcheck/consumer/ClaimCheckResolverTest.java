package com.codrite.claimcheck.consumer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import java.time.Instant;

class ClaimCheckResolverTest {

    static class FakePayloadReader implements PayloadReader {
        private final Map<String, String> docs = new HashMap<>();
        private final List<String> calls = new ArrayList<>();

        void put(String id, String val) { docs.put(id, val); }
        List<String> getCalls() { return calls; }

        @Override
        public Optional<String> fetch(String mongoId) {
            calls.add(mongoId);
            return Optional.ofNullable(docs.get(mongoId));
        }
    }

    @Test
    void inlineEnvelopeResolvesWithoutReader() {
        FakePayloadReader fake = new FakePayloadReader();
        ClaimCheckResolver resolver = new ClaimCheckResolver(fake);
        MessageEnvelope env = new MessageEnvelope("m1", "pair", Instant.now(), 0L, 5L, null, "hello", null);
        
        Optional<ResolvedMessage> res = resolver.resolve(env);
        
        assertTrue(res.isPresent());
        assertEquals("m1", res.get().messageId());
        assertEquals(DeliveryPath.INLINE, res.get().path());
        assertEquals("hello", res.get().payload());
        assertEquals(5L, res.get().sizeBytes());
        assertTrue(fake.getCalls().isEmpty());
    }

    @Test
    void claimCheckEnvelopeFetchesFromReader() {
        FakePayloadReader fake = new FakePayloadReader();
        fake.put("abc", "stored-payload");
        ClaimCheckResolver resolver = new ClaimCheckResolver(fake);
        MessageEnvelope env = new MessageEnvelope("m2", "pair", Instant.now(), 0L, 7L, null, null, "abc");
        
        Optional<ResolvedMessage> res = resolver.resolve(env);
        
        assertTrue(res.isPresent());
        assertEquals("m2", res.get().messageId());
        assertEquals(DeliveryPath.CLAIM_CHECK, res.get().path());
        assertEquals("stored-payload", res.get().payload());
        assertEquals(7L, res.get().sizeBytes());
        assertEquals(List.of("abc"), fake.getCalls());
    }

    @Test
    void missingDocumentReturnsEmpty() {
        FakePayloadReader fake = new FakePayloadReader();
        ClaimCheckResolver resolver = new ClaimCheckResolver(fake);
        MessageEnvelope env = new MessageEnvelope("m3", "pair", Instant.now(), 0L, 10L, null, null, "missing");
        
        Optional<ResolvedMessage> res = resolver.resolve(env);
        
        assertFalse(res.isPresent());
        assertEquals(List.of("missing"), fake.getCalls());
    }
}
