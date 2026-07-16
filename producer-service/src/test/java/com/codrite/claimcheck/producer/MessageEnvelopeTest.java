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
}
