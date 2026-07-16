package com.codrite.claimcheck.producer;

import org.springframework.stereotype.Component;
import org.springframework.kafka.core.KafkaTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import com.codrite.claimcheck.producer.DeliveryPath;
import com.codrite.claimcheck.producer.MessageEnvelope;
import com.codrite.claimcheck.producer.ProducerMetrics;

@Component
public final class EnvelopePublisher {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final ProducerMetrics metrics;

    public EnvelopePublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, ProducerMetrics metrics) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.metrics = metrics;
    }

    public void publish(MessageEnvelope env) {
        DeliveryPath path = env.mongoId() != null ? DeliveryPath.CLAIM_CHECK : DeliveryPath.INLINE;
        String json;
        try {
            json = mapper.writeValueAsString(env);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
        Timer.Sample sample = Timer.start();
        kafka.send("messages", env.pairId(), json).join();
        sample.stop(metrics.kafkaSend(path));
        metrics.recordMessage(path, env.payloadSizeBytes());
    }
}
