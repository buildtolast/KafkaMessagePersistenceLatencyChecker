package com.codrite.claimcheck.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.Document;
import org.bson.types.ObjectId;
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
class ConsumerIntegrationTest {

    private static final int CHAIN_LENGTH = 3;

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");
    @Container
    static MongoDBContainer mongo = new MongoDBContainer("mongo:7");

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    MeterRegistry registry;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) throws Exception {
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("spring.data.mongodb.uri", () -> mongo.getReplicaSetUrl() + "?directConnection=true");
        r.add("app.chain.length", () -> CHAIN_LENGTH);

        // Seed topic-01..topic-03 before the consumer's pattern-subscribed
        // listener starts, so its first assignment doesn't race a metadata
        // refresh (mirrors compose's topic-init-before-consumer ordering).
        Map<String, Object> adminConfig = new HashMap<>();
        adminConfig.put("bootstrap.servers", kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminConfig)) {
            List<NewTopic> topics = List.of(
                new NewTopic("topic-01", 3, (short) 1),
                new NewTopic("topic-02", 3, (short) 1),
                new NewTopic("topic-03", 3, (short) 1)
            );
            admin.createTopics(topics).all().get();
        }
    }

    @Test
    void chainRelaysBothPathsFromTopic01ToTerminalStage() throws Exception {
        ObjectId oid = new ObjectId();
        Document doc = new Document("_id", oid)
            .append("payload", "big-payload-value")
            .append("sizeBytes", 18L)
            .append("createdAt", java.util.Date.from(Instant.now()));
        mongoTemplate.insert(doc, "large_payloads");

        MessageEnvelope inline = new MessageEnvelope("m-inline", "p1", Instant.now(), nowEpochNanos(), 3L, DeliveryPath.INLINE, "abc", null);
        MessageEnvelope claim = new MessageEnvelope("m-claim", "p1", Instant.now(), nowEpochNanos(), 18L, DeliveryPath.CLAIM_CHECK, null, oid.toHexString());

        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        String inlineJson = mapper.writeValueAsString(inline);
        String claimJson = mapper.writeValueAsString(claim);

        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put("bootstrap.servers", kafka.getBootstrapServers());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig, new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>("topic-01", inlineJson)).get();
            producer.send(new ProducerRecord<>("topic-01", claimJson)).get();
        }

        double inlineTerminalLatencyCount = 0.0;
        double claimTerminalLatencyCount = 0.0;
        for (int i = 0; i < 60; i++) {
            inlineTerminalLatencyCount = registry.find("chain.e2e.latency").tag("path", "INLINE").timer() != null
                ? registry.find("chain.e2e.latency").tag("path", "INLINE").timer().count() : 0.0;
            claimTerminalLatencyCount = registry.find("chain.e2e.latency").tag("path", "CLAIM_CHECK").timer() != null
                ? registry.find("chain.e2e.latency").tag("path", "CLAIM_CHECK").timer().count() : 0.0;
            if (inlineTerminalLatencyCount >= 1.0 && claimTerminalLatencyCount >= 1.0) {
                break;
            }
            Thread.sleep(500L);
        }

        assertThat(inlineTerminalLatencyCount)
            .as("inline message should reach the terminal stage (topic-03) and record chain-total latency")
            .isGreaterThanOrEqualTo(1.0);
        assertThat(claimTerminalLatencyCount)
            .as("claim-check message should reach the terminal stage (topic-03) and record chain-total latency")
            .isGreaterThanOrEqualTo(1.0);

        // Claim-check re-checks at stage 1 and stage 2 (non-terminal), each
        // inserting a NEW document; stage 3 (terminal) does not insert. So
        // large_payloads should now hold the original doc plus 2 new ones.
        assertThat(mongoTemplate.count(new Query(), "large_payloads"))
            .as("non-terminal claim-check hops each insert a new document; terminal hop does not")
            .isGreaterThanOrEqualTo(3L);
    }

    private static long nowEpochNanos() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }
}
