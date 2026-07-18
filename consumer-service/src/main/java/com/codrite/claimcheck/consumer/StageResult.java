package com.codrite.claimcheck.consumer;

public sealed interface StageResult permits StageResult.Republish, StageResult.Terminal, StageResult.Skipped {

    record Republish(MessageEnvelope envelope, String targetTopic) implements StageResult {}

    record Terminal(long chainTotalLatencyNanos, MessageEnvelope finalEnvelope) implements StageResult {}

    record Skipped(String reason) implements StageResult {}
}
