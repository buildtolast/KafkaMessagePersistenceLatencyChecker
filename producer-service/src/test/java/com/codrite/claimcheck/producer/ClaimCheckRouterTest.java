package com.codrite.claimcheck.producer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.codrite.claimcheck.producer.ClaimCheckRouter;
import com.codrite.claimcheck.producer.DeliveryPath;
import com.codrite.claimcheck.producer.MessageEnvelope;
import com.codrite.claimcheck.producer.PayloadStore;

public class ClaimCheckRouterTest {

    private static final long THRESHOLD = 2097152L;
    private static final String OVERSIZED_PAYLOAD = "x".repeat(2097153);
    private static final String EXACT_PAYLOAD = "x".repeat(2097152);

    @Test
    void exactlyThresholdIsInline() {
        ClaimCheckRouter router = new ClaimCheckRouter(THRESHOLD, new RecordingStore());
        DeliveryPath result = router.decide(THRESHOLD, null);
        assertThat(result).isEqualTo(DeliveryPath.INLINE);
    }

    @Test
    void oneByteOverThresholdIsClaimCheck() {
        ClaimCheckRouter router = new ClaimCheckRouter(THRESHOLD, new RecordingStore());
        DeliveryPath result = router.decide(THRESHOLD + 1, null);
        assertThat(result).isEqualTo(DeliveryPath.CLAIM_CHECK);
    }

    @Test
    void forcedInlineOverridesSizeAndSkipsStore() {
        RecordingStore store = new RecordingStore();
        ClaimCheckRouter router = new ClaimCheckRouter(THRESHOLD, store);
        MessageEnvelope env = router.route("m", "p", OVERSIZED_PAYLOAD, DeliveryPath.INLINE);

        assertThat(env.payload()).isNotNull();
        assertThat(env.mongoId()).isNull();
        assertThat(store.callCount).isEqualTo(0);
    }

    @Test
    void forcedClaimCheckStoresTinyPayload() {
        RecordingStore store = new RecordingStore();
        ClaimCheckRouter router = new ClaimCheckRouter(THRESHOLD, store);
        MessageEnvelope env = router.route("m", "p", "hi", DeliveryPath.CLAIM_CHECK);

        assertThat(env.mongoId()).isEqualTo("abc123");
        assertThat(env.payload()).isNull();
        assertThat(store.lastPayload).isEqualTo("hi");
        assertThat(store.lastSize).isEqualTo(2);
        assertThat(store.lastStage).isEqualTo(0);
    }

    @Test
    void inlineRouteLeavesStoreUntouched() {
        RecordingStore store = new RecordingStore();
        ClaimCheckRouter router = new ClaimCheckRouter(THRESHOLD, store);
        MessageEnvelope env = router.route("m", "p", "hello", null);

        assertThat(env.payload()).isEqualTo("hello");
        assertThat(env.mongoId()).isNull();
        assertThat(store.callCount).isEqualTo(0);
        assertThat(env.payloadSizeBytes()).isEqualTo(5);
        assertThat(env.messageId()).isEqualTo("m");
        assertThat(env.pairId()).isEqualTo("p");
    }

    static class RecordingStore implements PayloadStore {
        String lastPayload;
        long lastSize;
        int lastStage;
        int callCount = 0;

        @Override
        public String store(String payload, long size, int stage) {
            lastPayload = payload;
            lastSize = size;
            lastStage = stage;
            callCount++;
            return "abc123";
        }
    }
}
