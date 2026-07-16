package com.codrite.claimcheck.consumer;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record MessageEnvelope(
    String messageId,
    String pairId,
    java.time.Instant createdAt,
    long producedAtEpochNanos,
    long payloadSizeBytes,
    DeliveryPath forcedPath,
    String payload,
    String mongoId
) {}
