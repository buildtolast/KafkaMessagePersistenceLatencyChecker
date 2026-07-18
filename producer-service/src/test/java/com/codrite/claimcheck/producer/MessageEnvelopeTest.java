package com.codrite.claimcheck.producer;

import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.assertj.core.api.Assertions;
import java.time.Instant;

public class MessageEnvelopeTest {

    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    public void jsonRoundTripInlineEnvelope() throws Exception {
        String json = mapper.writeValueAsString(new MessageEnvelope("msg1", "pair1", Instant.parse("2026-07-15T10:00:00Z"), 123456789L, 5, DeliveryPath.INLINE, "hello", null));
        MessageEnvelope result = mapper.readValue(json, MessageEnvelope.class);
        Assertions.assertThat(result).isEqualTo(new MessageEnvelope("msg1", "pair1", Instant.parse("2026-07-15T10:00:00Z"), 123456789L, 5, DeliveryPath.INLINE, "hello", null));
    }

    @Test
    public void jsonRoundTripClaimCheckEnvelope() throws Exception {
        String json = mapper.writeValueAsString(new MessageEnvelope("msg2", "pair2", Instant.parse("2026-07-15T10:00:00Z"), 123456789L, 0, null, null, "665f1a2b3c4d5e6f77889900"));
        MessageEnvelope result = mapper.readValue(json, MessageEnvelope.class);
        Assertions.assertThat(result).isEqualTo(new MessageEnvelope("msg2", "pair2", Instant.parse("2026-07-15T10:00:00Z"), 123456789L, 0, null, null, "665f1a2b3c4d5e6f77889900"));
    }

    @Test
    public void inlineJsonOmitsNullFields() throws Exception {
        String json = mapper.writeValueAsString(new MessageEnvelope("msg3", "pair3", Instant.parse("2026-07-15T10:00:00Z"), 123456789L, 5, DeliveryPath.INLINE, "hello", null));
        Assertions.assertThat(json).doesNotContain("mongoId");
    }

    @Test
    public void legacyConstructorDefaultsToEmptyHopTrace() {
        MessageEnvelope envelope = new MessageEnvelope("msg4", "pair4", Instant.parse("2026-07-15T10:00:00Z"), 123456789L, 5, DeliveryPath.INLINE, "hello", null);
        Assertions.assertThat(envelope.hopTrace()).isEmpty();
    }

    @Test
    public void jsonRoundTripPreservesHopTrace() throws Exception {
        java.util.List<HopRecord> trace = java.util.List.of(
            new HopRecord(1, 1000L, 2000L),
            new HopRecord(2, 3000L, null)
        );
        MessageEnvelope envelope = new MessageEnvelope("msg5", "pair5", Instant.parse("2026-07-15T10:00:00Z"), 123456789L, 5, DeliveryPath.INLINE, "hello", null, trace);

        String json = mapper.writeValueAsString(envelope);
        MessageEnvelope result = mapper.readValue(json, MessageEnvelope.class);

        Assertions.assertThat(result.hopTrace()).isEqualTo(trace);
    }

    @Test
    public void hopTraceIsDefensivelyCopiedAndImmutable() {
        java.util.List<HopRecord> mutable = new java.util.ArrayList<>();
        mutable.add(new HopRecord(1, 1000L, 2000L));
        MessageEnvelope envelope = new MessageEnvelope("msg6", "pair6", Instant.parse("2026-07-15T10:00:00Z"), 123456789L, 5, DeliveryPath.INLINE, "hello", null, mutable);

        mutable.add(new HopRecord(2, 3000L, 4000L));

        Assertions.assertThat(envelope.hopTrace()).hasSize(1);
        Assertions.assertThatThrownBy(() -> envelope.hopTrace().add(new HopRecord(3, 5000L, 6000L)))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void appendingHopProducesNewImmutableInstance() {
        MessageEnvelope original = new MessageEnvelope("msg7", "pair7", Instant.parse("2026-07-15T10:00:00Z"), 123456789L, 5, DeliveryPath.INLINE, "hello", null);
        HopRecord hop = new HopRecord(1, 1000L, 2000L);

        java.util.List<HopRecord> appended = new java.util.ArrayList<>(original.hopTrace());
        appended.add(hop);
        MessageEnvelope withHop = new MessageEnvelope(original.messageId(), original.pairId(), original.createdAt(), original.producedAtEpochNanos(), original.payloadSizeBytes(), original.forcedPath(), original.payload(), original.mongoId(), appended);

        Assertions.assertThat(original.hopTrace()).isEmpty();
        Assertions.assertThat(withHop.hopTrace()).containsExactly(hop);
        Assertions.assertThat(withHop).isNotSameAs(original);
    }
}
