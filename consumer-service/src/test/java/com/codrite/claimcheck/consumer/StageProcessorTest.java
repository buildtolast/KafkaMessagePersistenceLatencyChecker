package com.codrite.claimcheck.consumer;

import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class StageProcessorTest {

    static class FakePayloadReader implements PayloadReader {
        private final Map<String, String> docs = new HashMap<>();
        private final List<String> fetchCalls = new ArrayList<>();
        private final List<Integer> fetchStages = new ArrayList<>();

        void put(String id, String value) {
            docs.put(id, value);
        }

        List<String> fetchCalls() {
            return fetchCalls;
        }

        List<Integer> fetchStages() {
            return fetchStages;
        }

        @Override
        public Optional<String> fetch(String mongoId, int stage) {
            fetchCalls.add(mongoId);
            fetchStages.add(stage);
            return Optional.ofNullable(docs.get(mongoId));
        }
    }

    static class FakePayloadStore implements PayloadStore {
        private final List<String> storedPayloads = new ArrayList<>();
        private final List<Long> storedSizes = new ArrayList<>();
        private final List<Integer> storedStages = new ArrayList<>();
        private String nextId = "generated-id";

        void nextId(String id) {
            this.nextId = id;
        }

        List<String> storedPayloads() {
            return storedPayloads;
        }

        List<Long> storedSizes() {
            return storedSizes;
        }

        List<Integer> storedStages() {
            return storedStages;
        }

        int callCount() {
            return storedPayloads.size();
        }

        @Override
        public String store(String payload, long sizeBytes, int stage) {
            storedPayloads.add(payload);
            storedSizes.add(sizeBytes);
            storedStages.add(stage);
            return nextId;
        }
    }

    @Test
    void inlineHopTransformsRepublishesAndAppendsTrace() {
        FakePayloadReader reader = new FakePayloadReader();
        FakePayloadStore store = new FakePayloadStore();
        StageProcessor processor = new StageProcessor(reader, store);

        StageConfig config = new StageConfig(1, 3);
        Instant now = Instant.parse("2023-01-01T00:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);

        String payload = "hello-world";
        MessageEnvelope envelope = new MessageEnvelope(
                "msg-1", "pair-1", Instant.parse("2023-01-01T00:00:00Z"), 1000000L,
                payload.length(), DeliveryPath.INLINE, payload, null
        );

        StageResult result = processor.process(envelope, config, clock);

        assertThat(result).isInstanceOf(StageResult.Republish.class);
        StageResult.Republish republish = (StageResult.Republish) result;
        assertThat(republish.targetTopic()).isEqualTo("topic-02");

        MessageEnvelope out = republish.envelope();
        assertThat(out.payload()).isNotNull().hasSize(payload.length());
        assertThat(out.mongoId()).isNull();
        assertThat(out.hopTrace()).hasSize(1);
        assertThat(out.hopTrace().get(0).stage()).isEqualTo(1);
        assertThat(out.hopTrace().get(0).publishedAtEpochNanos()).isNotNull();
    }

    @Test
    void claimCheckHopFetchesTransformsInsertsNewIdAndRepublishes() {
        FakePayloadReader reader = new FakePayloadReader();
        FakePayloadStore store = new FakePayloadStore();
        StageProcessor processor = new StageProcessor(reader, store);

        String mongoId = "abc123";
        String storedValue = "stored-payload-1234"; // length 20
        reader.put(mongoId, storedValue);
        store.nextId("new-id-001");

        StageConfig config = new StageConfig(1, 3);
        Clock clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);

        MessageEnvelope envelope = new MessageEnvelope(
                "msg-1", "pair-1", Instant.now(), 1000000L,
                storedValue.length(), DeliveryPath.CLAIM_CHECK, null, mongoId
        );

        StageResult result = processor.process(envelope, config, clock);

        assertThat(result).isInstanceOf(StageResult.Republish.class);
        StageResult.Republish republish = (StageResult.Republish) result;
        assertThat(republish.targetTopic()).isEqualTo("topic-02");

        MessageEnvelope out = republish.envelope();
        assertThat(out.mongoId()).isEqualTo("new-id-001");
        assertThat(out.payload()).isNull();
        assertThat(reader.fetchCalls()).containsExactly(mongoId);
        assertThat(reader.fetchStages()).containsExactly(1);
        assertThat(store.callCount()).isEqualTo(1);
        assertThat(store.storedPayloads().get(0)).hasSize(storedValue.length());
        assertThat(store.storedStages()).containsExactly(1);
        assertThat(out.hopTrace()).hasSize(1);
        assertThat(out.hopTrace().get(0).stage()).isEqualTo(1);
    }

    @Test
    void terminalStageInlineRecordsChainTotalAndSkipsTransform() {
        FakePayloadReader reader = new FakePayloadReader();
        FakePayloadStore store = new FakePayloadStore();
        StageProcessor processor = new StageProcessor(reader, store);

        Instant now = Instant.parse("2023-01-01T00:00:00Z");
        long consumedAt = now.getEpochSecond() * 1_000_000_000L + now.getNano();
        long producedAt = consumedAt - 5_000_000L;
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);

        StageConfig config = new StageConfig(3, 3);
        String payload = "terminal-payload";
        MessageEnvelope envelope = new MessageEnvelope(
                "msg-1", "pair-1", now, producedAt,
                payload.length(), DeliveryPath.INLINE, payload, null
        );

        StageResult result = processor.process(envelope, config, clock);

        assertThat(result).isInstanceOf(StageResult.Terminal.class);
        StageResult.Terminal terminal = (StageResult.Terminal) result;
        assertThat(terminal.chainTotalLatencyNanos()).isEqualTo(5_000_000L);

        MessageEnvelope out = terminal.finalEnvelope();
        assertThat(out.payload()).isEqualTo(payload);
        assertThat(out.hopTrace()).hasSize(1);
        assertThat(out.hopTrace().get(0).stage()).isEqualTo(3);
        assertThat(out.hopTrace().get(0).publishedAtEpochNanos()).isNull();
    }

    @Test
    void terminalStageClaimCheckDoesNoMongoIO() {
        FakePayloadReader reader = new FakePayloadReader();
        FakePayloadStore store = new FakePayloadStore();
        StageProcessor processor = new StageProcessor(reader, store);

        StageConfig config = new StageConfig(3, 3);
        Clock clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);

        MessageEnvelope envelope = new MessageEnvelope(
                "msg-1", "pair-1", Instant.now(), 1000000L,
                10, DeliveryPath.CLAIM_CHECK, null, "abc-terminal"
        );

        StageResult result = processor.process(envelope, config, clock);

        assertThat(result).isInstanceOf(StageResult.Terminal.class);
        StageResult.Terminal terminal = (StageResult.Terminal) result;
        assertThat(terminal.finalEnvelope().mongoId()).isEqualTo("abc-terminal");
        assertThat(reader.fetchCalls()).isEmpty();
        assertThat(store.callCount()).isZero();
    }

    @Test
    void missingDocumentSkipsWithNoRetry() {
        FakePayloadReader reader = new FakePayloadReader();
        FakePayloadStore store = new FakePayloadStore();
        StageProcessor processor = new StageProcessor(reader, store);

        StageConfig config = new StageConfig(1, 3);
        Clock clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);

        MessageEnvelope envelope = new MessageEnvelope(
                "msg-1", "pair-1", Instant.now(), 1000000L,
                10, DeliveryPath.CLAIM_CHECK, null, "missing-id"
        );

        StageResult result = processor.process(envelope, config, clock);

        assertThat(result).isInstanceOf(StageResult.Skipped.class);
        assertThat(reader.fetchCalls()).containsExactly("missing-id");
        assertThat(store.callCount()).isZero();
    }

    @Test
    void stageBeforeTerminalBoundaryRepublishesToNextTopic() {
        FakePayloadReader reader = new FakePayloadReader();
        FakePayloadStore store = new FakePayloadStore();
        StageProcessor processor = new StageProcessor(reader, store);

        StageConfig config = new StageConfig(2, 3);
        Clock clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);

        MessageEnvelope envelope = new MessageEnvelope(
                "msg-1", "pair-1", Instant.now(), 1000000L,
                4, DeliveryPath.INLINE, "data", null
        );

        StageResult result = processor.process(envelope, config, clock);

        assertThat(result).isInstanceOf(StageResult.Republish.class);
        assertThat(((StageResult.Republish) result).targetTopic()).isEqualTo("topic-03");
    }

    @Test
    void stageNumberExceedingChainLengthIsAlsoTerminal() {
        FakePayloadReader reader = new FakePayloadReader();
        FakePayloadStore store = new FakePayloadStore();
        StageProcessor processor = new StageProcessor(reader, store);

        StageConfig config = new StageConfig(5, 3);
        Clock clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);

        MessageEnvelope envelope = new MessageEnvelope(
                "msg-1", "pair-1", Instant.now(), 1000000L,
                4, DeliveryPath.INLINE, "data", null
        );

        StageResult result = processor.process(envelope, config, clock);

        assertThat(result).isInstanceOf(StageResult.Terminal.class);
    }
}
