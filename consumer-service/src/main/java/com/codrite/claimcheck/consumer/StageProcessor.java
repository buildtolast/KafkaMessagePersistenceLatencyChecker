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
        Instant now = clock.instant();
        long consumedAtEpochNanos = now.getEpochSecond() * 1_000_000_000L + now.getNano();

        if (config.isTerminal()) {
            long chainTotalLatencyNanos = consumedAtEpochNanos - envelope.producedAtEpochNanos();
            List<HopRecord> newTrace = new ArrayList<>(envelope.hopTrace());
            newTrace.add(new HopRecord(config.stageNumber(), consumedAtEpochNanos, null));

            MessageEnvelope terminalEnvelope = new MessageEnvelope(
                envelope.messageId(),
                envelope.pairId(),
                envelope.createdAt(),
                envelope.producedAtEpochNanos(),
                envelope.payloadSizeBytes(),
                envelope.forcedPath(),
                envelope.payload(),
                envelope.mongoId(),
                newTrace
            );
            return new StageResult.Terminal(chainTotalLatencyNanos, terminalEnvelope);
        }

        if (envelope.mongoId() != null) {
            Optional<String> fetched = reader.fetch(envelope.mongoId(), config.stageNumber());
            if (fetched.isEmpty()) {
                return new StageResult.Skipped("missing document: " + envelope.mongoId());
            }

            String transformed = transform(fetched.get());
            String newMongoId = store.store(transformed, envelope.payloadSizeBytes(), config.stageNumber());

            Instant pubNow = clock.instant();
            long publishedAtEpochNanos = pubNow.getEpochSecond() * 1_000_000_000L + pubNow.getNano();

            List<HopRecord> newTrace = new ArrayList<>(envelope.hopTrace());
            newTrace.add(new HopRecord(config.stageNumber(), consumedAtEpochNanos, publishedAtEpochNanos));

            MessageEnvelope nextEnvelope = new MessageEnvelope(
                envelope.messageId(),
                envelope.pairId(),
                envelope.createdAt(),
                envelope.producedAtEpochNanos(),
                envelope.payloadSizeBytes(),
                envelope.forcedPath(),
                null,
                newMongoId,
                newTrace
            );
            return new StageResult.Republish(nextEnvelope, config.nextTopic());
        } else {
            String transformed = transform(envelope.payload());

            Instant pubNow = clock.instant();
            long publishedAtEpochNanos = pubNow.getEpochSecond() * 1_000_000_000L + pubNow.getNano();

            List<HopRecord> newTrace = new ArrayList<>(envelope.hopTrace());
            newTrace.add(new HopRecord(config.stageNumber(), consumedAtEpochNanos, publishedAtEpochNanos));

            MessageEnvelope nextEnvelope = new MessageEnvelope(
                envelope.messageId(),
                envelope.pairId(),
                envelope.createdAt(),
                envelope.producedAtEpochNanos(),
                envelope.payloadSizeBytes(),
                envelope.forcedPath(),
                transformed,
                null,
                newTrace
            );
            return new StageResult.Republish(nextEnvelope, config.nextTopic());
        }
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
