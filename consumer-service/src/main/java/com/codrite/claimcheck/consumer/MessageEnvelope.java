package com.codrite.claimcheck.consumer;

import com.fasterxml.jackson.annotation.JsonInclude;
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
        String mongoId,
        List<HopRecord> hopTrace
    ) {
        this.messageId = messageId;
        this.pairId = pairId;
        this.createdAt = createdAt;
        this.producedAtEpochNanos = producedAtEpochNanos;
        this.payloadSizeBytes = payloadSizeBytes;
        this.forcedPath = forcedPath;
        this.payload = payload;
        this.mongoId = mongoId;
        this.hopTrace = List.copyOf(hopTrace);
    }

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
}
