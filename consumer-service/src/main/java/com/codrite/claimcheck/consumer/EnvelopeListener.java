package com.codrite.claimcheck.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

@Component
public final class EnvelopeListener {
    private static final Logger log = LoggerFactory.getLogger(EnvelopeListener.class);

    private final StageProcessor stageProcessor;
    private final ConsumerMetrics metrics;
    private final ObjectMapper mapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TraceBuffer traceBuffer;
    private final int chainLength;

    public EnvelopeListener(
            StageProcessor stageProcessor,
            ConsumerMetrics metrics,
            ObjectMapper mapper,
            KafkaTemplate<String, String> kafkaTemplate,
            TraceBuffer traceBuffer,
            @Value("${app.chain.length}") int chainLength) {
        this.stageProcessor = stageProcessor;
        this.metrics = metrics;
        this.mapper = mapper;
        this.kafkaTemplate = kafkaTemplate;
        this.traceBuffer = traceBuffer;
        this.chainLength = chainLength;
    }

    @KafkaListener(topicPattern = "topic-\\d{2}", groupId = "claim-check-demo")
    public void onMessage(String value, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        long hopStartNanos = System.nanoTime();
        try {
            MessageEnvelope envelope = mapper.readValue(value, MessageEnvelope.class);
            int stageNumber = parseStageNumber(topic);
            StageConfig config = new StageConfig(stageNumber, chainLength);
            StageResult result = stageProcessor.process(envelope, config, Clock.systemUTC());

            switch (result) {
                case StageResult.Republish r -> handleRepublish(r, stageNumber, hopStartNanos);
                case StageResult.Terminal t -> handleTerminal(t, stageNumber, hopStartNanos);
                case StageResult.Skipped s -> log.error("Message skipped: {}", s.reason());
            }
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize MessageEnvelope from topic {}: {}", topic, e.getMessage());
        }
    }

    private void handleRepublish(StageResult.Republish r, int stageNumber, long hopStartNanos) {
        try {
            String json = mapper.writeValueAsString(r.envelope());
            kafkaTemplate.send(r.targetTopic(), r.envelope().messageId(), json).get();

            DeliveryPath path = pathOf(r.envelope());
            metrics.recordMessage(path, r.envelope().payloadSizeBytes());
            metrics.recordStageHop(stageNumber, path, Duration.ofNanos(System.nanoTime() - hopStartNanos));

            log.info("Stage {}: Republished to {} [Path: {}]", stageNumber, r.targetTopic(), path);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to republish message {}: {}", r.envelope().messageId(), e.getMessage());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize envelope for republish: {}", e.getMessage());
        }
    }

    private void handleTerminal(StageResult.Terminal t, int stageNumber, long hopStartNanos) {
        DeliveryPath path = pathOf(t.finalEnvelope());
        metrics.recordE2e(path, Duration.ofNanos(t.chainTotalLatencyNanos()));
        metrics.recordMessage(path, t.finalEnvelope().payloadSizeBytes());
        metrics.recordStageHop(stageNumber, path, Duration.ofNanos(System.nanoTime() - hopStartNanos));
        traceBuffer.record(new TraceRecord(t.finalEnvelope().pairId(), path, t.finalEnvelope().hopTrace()));

        log.info("Terminal stage reached. MessageId: {}, Path: {}, Latency: {}ms",
                t.finalEnvelope().messageId(), path, t.chainTotalLatencyNanos() / 1_000_000.0);
    }

    private static int parseStageNumber(String topic) {
        return Integer.parseInt(topic.substring(topic.length() - 2));
    }

    private static DeliveryPath pathOf(MessageEnvelope env) {
        return env.mongoId() != null ? DeliveryPath.CLAIM_CHECK : DeliveryPath.INLINE;
    }
}
