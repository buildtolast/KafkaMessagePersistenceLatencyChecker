package com.codrite.claimcheck.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageEnvelopeTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .build();
    }

    @Test
    void jsonRoundTripInlineEnvelope() throws Exception {
        Instant now = Instant.now();
        MessageEnvelope original = new MessageEnvelope(
                "msg-1", "pair-1", now, 1000L, 5L,
                DeliveryPath.INLINE, "hello", null
        );

        String json = objectMapper.writeValueAsString(original);
        MessageEnvelope deserialized = objectMapper.readValue(json, MessageEnvelope.class);

        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void jsonRoundTripClaimCheckEnvelope() throws Exception {
        Instant now = Instant.now();
        MessageEnvelope original = new MessageEnvelope(
                "msg-2", "pair-2", now, 2000L, 1024L,
                null, null, "65f1a2b3c4d5e6f7a8b9c0d1"
        );

        String json = objectMapper.writeValueAsString(original);
        MessageEnvelope deserialized = objectMapper.readValue(json, MessageEnvelope.class);

        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void legacyConstructorDefaultsToEmptyHopTrace() {
        MessageEnvelope envelope = new MessageEnvelope(
                "msg-3", "pair-3", Instant.now(), 3000L, 10L,
                DeliveryPath.INLINE, "data", "mongo-id"
        );

        assertThat(envelope.hopTrace()).isEmpty();
    }

    @Test
    void jsonRoundTripPreservesHopTrace() throws Exception {
        List<HopRecord> hops = List.of(
                new HopRecord(1, 100L, 110L),
                new HopRecord(2, 120L, null)
        );
        MessageEnvelope original = new MessageEnvelope(
                "msg-4", "pair-4", Instant.now(), 4000L, 10L,
                DeliveryPath.CLAIM_CHECK, "data", "mongo-id", hops
        );

        String json = objectMapper.writeValueAsString(original);
        MessageEnvelope deserialized = objectMapper.readValue(json, MessageEnvelope.class);

        assertThat(deserialized.hopTrace()).isEqualTo(hops);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void hopTraceIsDefensivelyCopiedAndImmutable() {
        List<HopRecord> mutableHops = new ArrayList<>();
        mutableHops.add(new HopRecord(1, 100L, 110L));

        MessageEnvelope envelope = new MessageEnvelope(
                "msg-5", "pair-5", Instant.now(), 5000L, 10L,
                DeliveryPath.INLINE, "data", "mongo-id", mutableHops
        );

        // Mutate original list
        mutableHops.add(new HopRecord(2, 200L, 210L));

        // Assert envelope is unaffected
        assertThat(envelope.hopTrace()).hasSize(1);

        // Assert the returned list is immutable
        assertThatThrownBy(() -> envelope.hopTrace().add(new HopRecord(3, 300L, 310L)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void appendingHopProducesNewImmutableInstance() {
        MessageEnvelope original = new MessageEnvelope(
                "msg-6", "pair-6", Instant.now(), 6000L, 10L,
                DeliveryPath.INLINE, "data", "mongo-id"
        );

        List<HopRecord> newHops = new ArrayList<>(original.hopTrace());
        newHops.add(new HopRecord(1, 7000L, 7100L));

        MessageEnvelope updated = new MessageEnvelope(
                original.messageId(),
                original.pairId(),
                original.createdAt(),
                original.producedAtEpochNanos(),
                original.payloadSizeBytes(),
                original.forcedPath(),
                original.payload(),
                original.mongoId(),
                newHops
        );

        assertThat(original.hopTrace()).isEmpty();
        assertThat(updated.hopTrace()).hasSize(1);
        assertThat(updated.hopTrace().get(0)).isEqualTo(newHops.get(0));
        assertThat(updated).isNotSameAs(original);
    }
}
