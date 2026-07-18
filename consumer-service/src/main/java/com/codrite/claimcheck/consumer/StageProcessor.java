package com.codrite.claimcheck.consumer;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class StageProcessor {

    private final PayloadReader reader;
    private final PayloadStore store;

    public StageProcessor(PayloadReader reader, PayloadStore store) {
        this.reader = reader;
        this.store = store;
    }

    public StageResult process(MessageEnvelope envelope, StageConfig config, Clock clock) {
        long consumedAtEpochNanos = epochNanos(clock);

        if (config.isTerminal()) {
            long chainTotalLatencyNanos = consumedAtEpochNanos - envelope.producedAtEpochNanos();
            HopRecord hop = new HopRecord(config.stageNumber(), consumedAtEpochNanos, null);
            MessageEnvelope terminalEnvelope = withHop(envelope, hop, envelope.payload(), envelope.mongoId());
            return new StageResult.Terminal(chainTotalLatencyNanos, terminalEnvelope);
        }

        String targetTopic = config.nextTopic()
            .orElseThrow(() -> new IllegalStateException("non-terminal stage " + config.stageNumber() + " has no next topic"));

        if (envelope.mongoId() != null) {
            Optional<String> fetched = reader.fetch(envelope.mongoId(), config.stageNumber());
            if (fetched.isEmpty()) {
                return new StageResult.Skipped("missing document: " + envelope.mongoId());
            }

            String transformed = transform(fetched.get());
            String newMongoId = store.store(transformed, envelope.payloadSizeBytes(), config.stageNumber());
            long publishedAtEpochNanos = epochNanos(clock);

            HopRecord hop = new HopRecord(config.stageNumber(), consumedAtEpochNanos, publishedAtEpochNanos);
            MessageEnvelope nextEnvelope = withHop(envelope, hop, null, newMongoId);
            return new StageResult.Republish(nextEnvelope, targetTopic);
        } else {
            String transformed = transform(envelope.payload());
            long publishedAtEpochNanos = epochNanos(clock);

            HopRecord hop = new HopRecord(config.stageNumber(), consumedAtEpochNanos, publishedAtEpochNanos);
            MessageEnvelope nextEnvelope = withHop(envelope, hop, transformed, null);
            return new StageResult.Republish(nextEnvelope, targetTopic);
        }
    }

    private static long epochNanos(Clock clock) {
        Instant instant = clock.instant();
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }

    private static MessageEnvelope withHop(MessageEnvelope envelope, HopRecord hop, String payload, String mongoId) {
        List<HopRecord> newTrace = new ArrayList<>(envelope.hopTrace());
        newTrace.add(hop);
        return new MessageEnvelope(
            envelope.messageId(),
            envelope.pairId(),
            envelope.createdAt(),
            envelope.producedAtEpochNanos(),
            envelope.payloadSizeBytes(),
            envelope.forcedPath(),
            payload,
            mongoId,
            newTrace
        );
    }

    private String transform(String payload) {
        if (payload == null || payload.isEmpty()) {
            return payload;
        }
        String marker = "STAGEMRK";
        int len = payload.length();
        if (len <= 8) {
            return marker.substring(0, len);
        } else {
            return marker + payload.substring(8);
        }
    }
}
