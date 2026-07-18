package com.codrite.claimcheck.consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TraceBufferTest {

    private TraceBuffer traceBuffer;

    @BeforeEach
    void setUp() {
        traceBuffer = new TraceBuffer();
    }

    @Test
    void latestReturnsEmptyWhenNothingRecorded() {
        Optional<TraceRecord> result = traceBuffer.latest(DeliveryPath.INLINE);
        assertThat(result).isEmpty();
    }

    @Test
    void recordThenLatestReturnsTheRecordedTrace() {
        HopRecord hop = new HopRecord(1, 1000L, 1100L);
        TraceRecord trace = new TraceRecord("p1", DeliveryPath.INLINE, List.of(hop));

        traceBuffer.record(trace);

        Optional<TraceRecord> result = traceBuffer.latest(DeliveryPath.INLINE);
        assertThat(result).isPresent().contains(trace);
    }

    @Test
    void recordingANewerTraceForTheSamePathOverwritesThePrevious() {
        TraceRecord firstTrace = new TraceRecord("p1", DeliveryPath.CLAIM_CHECK, List.of());
        TraceRecord secondTrace = new TraceRecord("p2", DeliveryPath.CLAIM_CHECK, List.of());

        traceBuffer.record(firstTrace);
        traceBuffer.record(secondTrace);

        Optional<TraceRecord> result = traceBuffer.latest(DeliveryPath.CLAIM_CHECK);
        assertThat(result).isPresent().contains(secondTrace);
        assertThat(result.get().pairId()).isEqualTo("p2");
    }

    @Test
    void inlineAndClaimCheckTracesAreTrackedIndependently() {
        TraceRecord inlineTrace = new TraceRecord("inline-1", DeliveryPath.INLINE, List.of());
        TraceRecord claimTrace = new TraceRecord("claim-1", DeliveryPath.CLAIM_CHECK, List.of());

        traceBuffer.record(inlineTrace);
        traceBuffer.record(claimTrace);

        Optional<TraceRecord> inlineResult = traceBuffer.latest(DeliveryPath.INLINE);
        Optional<TraceRecord> claimResult = traceBuffer.latest(DeliveryPath.CLAIM_CHECK);

        assertThat(inlineResult).isPresent().contains(inlineTrace);
        assertThat(claimResult).isPresent().contains(claimTrace);
    }
}
