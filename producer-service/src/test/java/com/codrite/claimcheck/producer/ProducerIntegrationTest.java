package com.codrite.claimcheck.producer;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
@SpringBootTest
public class ProducerIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

    @Container
    static MongoDBContainer mongo = new MongoDBContainer("mongo:7");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("spring.data.mongodb.uri", () -> mongo.getReplicaSetUrl() + "?directConnection=true");
        r.add("app.load.payload-bytes", () -> 64);
        r.add("app.load.tick-millis", () -> 200);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @Test
    void pairIsSplitAcrossBothPaths() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", kafka.getBootstrapServers());
        config.put("group.id", "it-test");
        config.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of("topic-01"));

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        Map<String, List<MessageEnvelope>> grouped = new HashMap<>();

        try {
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < 30000) {
                for (ConsumerRecord<String, String> rec : consumer.poll(Duration.ofMillis(500))) {
                    MessageEnvelope env = mapper.readValue(rec.value(), MessageEnvelope.class);
                    grouped.computeIfAbsent(env.pairId(), k -> new ArrayList<>()).add(env);
                }
                boolean found = false;
                for (List<MessageEnvelope> envelopes : grouped.values()) {
                    if (envelopes.size() == 2) {
                        found = true;
                        break;
                    }
                }
                if (found) break;
            }

            MessageEnvelope inline = null;
            MessageEnvelope claimCheck = null;

            for (List<MessageEnvelope> envelopes : grouped.values()) {
                if (envelopes.size() == 2) {
                    for (MessageEnvelope env : envelopes) {
                        if (env.payload() != null) {
                            inline = env;
                        } else if (env.mongoId() != null) {
                            claimCheck = env;
                        }
                    }
                    break;
                }
            }

            assertThat(inline).isNotNull();
            assertThat(claimCheck).isNotNull();

            assertThat(inline.payload()).isNotNull();
            assertThat(inline.mongoId()).isNull();

            assertThat(claimCheck.payload()).isNull();
            assertThat(claimCheck.mongoId()).isNotNull();

            assertThat(mongoTemplate.count(new Query(), "large_payloads")).isGreaterThan(0);

        } finally {
            consumer.close();
        }
    }
}
