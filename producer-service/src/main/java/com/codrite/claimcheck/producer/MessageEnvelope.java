package com.codrite.claimcheck.producer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.codrite.claimcheck.producer.DeliveryPath;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record MessageEnvelope(
    String messageId,
    String pairId,
    java.time.Instant createdAt,
    long producedAtEpochNanos,
    long payloadSizeBytes,
    DeliveryPath forcedPath,
    String payload,
    String mongoId,
    List<HopRecord> hopTrace
) {
    public MessageEnvelope(
        String messageId,
        String pairId,
        java.time.Instant createdAt,
        long producedAtEpochNanos,
        long payloadSizeBytes,
        DeliveryPath forcedPath,
        String payload,
        String mongoId
    ) {
        this(messageId, pairId, createdAt, producedAtEpochNanos, payloadSizeBytes, forcedPath, payload, mongoId, List.of());
    }

    public MessageEnvelope {
        hopTrace = List.copyOf(hopTrace);
    }
}
