package com.codrite.claimcheck.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
@SpringBootTest
class ConsumerIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");
    @Container
    static MongoDBContainer mongo = new MongoDBContainer("mongo:7");

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    MeterRegistry registry;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("spring.data.mongodb.uri", () -> mongo.getReplicaSetUrl() + "?directConnection=true");
    }

    @Test
    void bothPathsConsumedAndCounted() throws Exception {
        ObjectId oid = new ObjectId();
        Document doc = new Document("_id", oid).append("payload", "big-payload").append("sizeBytes", 11L);
        mongoTemplate.insert(doc, "large_payloads");

        MessageEnvelope inline = new MessageEnvelope("m-inline", "p1", Instant.now(), System.nanoTime(), 3L, null, "abc", null);
        MessageEnvelope claim = new MessageEnvelope("m-claim", "p1", Instant.now(), System.nanoTime(), 11L, null, null, oid.toHexString());

        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        String inlineJson = mapper.writeValueAsString(inline);
        String claimJson = mapper.writeValueAsString(claim);

        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", kafka.getBootstrapServers());
        KafkaProducer<String, String> producer = new KafkaProducer<>(config, new StringSerializer(), new StringSerializer());

        try {
            producer.send(new ProducerRecord<>("messages", inlineJson)).get();
            producer.send(new ProducerRecord<>("messages", claimJson)).get();
        } finally {
            producer.close();
        }

        double inlineCount = 0.0;
        double claimCount = 0.0;
        for (int i = 0; i < 60; i++) {
            inlineCount = registry.counter("consumer.messages", "path", "INLINE").count();
            claimCount = registry.counter("consumer.messages", "path", "CLAIM_CHECK").count();
            if (inlineCount >= 1.0 && claimCount >= 1.0) {
                break;
            }
            Thread.sleep(500L);
        }

        assertThat(inlineCount).isGreaterThanOrEqualTo(1.0);
        assertThat(claimCount).isGreaterThanOrEqualTo(1.0);
        assertThat(registry.counter("consumer.bytes", "path", "INLINE").count()).isGreaterThanOrEqualTo(3.0);
        assertThat(registry.counter("consumer.bytes", "path", "CLAIM_CHECK").count()).isGreaterThanOrEqualTo(11.0);
    }
}
