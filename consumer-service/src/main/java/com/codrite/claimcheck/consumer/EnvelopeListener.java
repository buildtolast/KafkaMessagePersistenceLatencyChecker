package com.codrite.claimcheck.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

@Component
public final class EnvelopeListener {
    private static final Logger log = LoggerFactory.getLogger(EnvelopeListener.class);

    private final ClaimCheckResolver resolver;
    private final ConsumerMetrics metrics;
    private final ObjectMapper mapper;

    public EnvelopeListener(ClaimCheckResolver resolver, ConsumerMetrics metrics, ObjectMapper mapper) {
        this.resolver = resolver;
        this.metrics = metrics;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "messages", groupId = "claim-check-demo")
    public void onMessage(String value) {
        MessageEnvelope env;
        try {
            env = mapper.readValue(value, MessageEnvelope.class);
        } catch (JsonProcessingException e) {
            log.error("failed to deserialize envelope", e);
            return;
        }

        long startNanos = System.nanoTime();
        Optional<ResolvedMessage> resolved = resolver.resolve(env);

        if (resolved.isEmpty()) {
            log.error("missing claim-check document mongoId={} messageId={}", env.mongoId(), env.messageId());
            return;
        }

        ResolvedMessage msg = resolved.get();
        Duration processing = Duration.ofNanos(System.nanoTime() - startNanos);
        Instant now = Instant.now();
        long nowEpochNanos = now.getEpochSecond() * 1_000_000_000L + now.getNano();
        Duration e2e = Duration.ofNanos(Math.max(0L, nowEpochNanos - env.producedAtEpochNanos()));

        metrics.recordProcessing(msg.path(), processing);
        metrics.recordE2e(msg.path(), e2e);
        metrics.recordMessage(msg.path(), msg.sizeBytes());

        log.info("consumed messageId={} path={} sizeBytes={} e2eMs={}",
                msg.messageId(), msg.path(), msg.sizeBytes(), e2e.toMillis());
    }
}
