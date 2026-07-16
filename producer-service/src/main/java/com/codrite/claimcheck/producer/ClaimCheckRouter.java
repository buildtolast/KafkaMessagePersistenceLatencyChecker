package com.codrite.claimcheck.producer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

public final class ClaimCheckRouter {
    private final long thresholdBytes;
    private final PayloadStore store;

    public ClaimCheckRouter(long thresholdBytes, PayloadStore store) {
        this.thresholdBytes = thresholdBytes;
        this.store = store;
    }

    public DeliveryPath decide(long payloadSizeBytes, DeliveryPath forcedPath) {
        if (forcedPath != null) return forcedPath;
        return payloadSizeBytes > thresholdBytes ? DeliveryPath.CLAIM_CHECK : DeliveryPath.INLINE;
    }

    public MessageEnvelope route(String messageId, String pairId, String payload, DeliveryPath forcedPath) {
        int size = payload.getBytes(StandardCharsets.UTF_8).length;
        DeliveryPath path = decide(size, forcedPath);
        Instant createdAt = Instant.now();
        long producedAtEpochNanos = createdAt.getEpochSecond() * 1_000_000_000L + createdAt.getNano();

        if (path == DeliveryPath.CLAIM_CHECK) {
            String mongoId = store.store(payload, size);
            return new MessageEnvelope(messageId, pairId, createdAt, producedAtEpochNanos, size, forcedPath, null, mongoId);
        } else {
            return new MessageEnvelope(messageId, pairId, createdAt, producedAtEpochNanos, size, forcedPath, payload, null);
        }
    }
}
