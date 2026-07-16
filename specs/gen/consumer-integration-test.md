Write a JUnit 5 Spring Boot integration test using Testcontainers.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text, NO explanation, and NO additional code blocks
before or after it.

Output path: consumer-service/src/test/java/com/codrite/claimcheck/consumer/ConsumerIntegrationTest.java
Package: com.codrite.claimcheck.consumer

ALREADY EXIST in this package (do NOT declare any of them): enum DeliveryPath
{ INLINE, CLAIM_CHECK }; record MessageEnvelope(String messageId, String pairId,
java.time.Instant createdAt, long producedAtEpochNanos, long payloadSizeBytes,
DeliveryPath forcedPath, String payload, String mongoId); ConsumerApplication
(the @SpringBootApplication class); EnvelopeListener; ConsumerMetrics;
MongoPayloadReader.

Output EXACTLY ONE top-level class ConsumerIntegrationTest. ONE fenced code block,
max 120 lines.

IMPORT RULES: begin the file with EXACTLY this import block, copied verbatim:

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

Note the first line is a STATIC import. NO wildcard imports. NO other assertion
classes. NO method references to methods throwing checked exceptions. NO streams.

Class annotations: @Testcontainers @SpringBootTest
Static containers:
@Container static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");
@Container static MongoDBContainer mongo = new MongoDBContainer("mongo:7");

@DynamicPropertySource static void props(DynamicPropertyRegistry r):
  r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  r.add("spring.data.mongodb.uri", () -> mongo.getReplicaSetUrl() + "?directConnection=true");
  (the directConnection parameter is REQUIRED — replica-set discovery breaks port mapping)

Autowire MongoTemplate and MeterRegistry as fields.

One @Test bothPathsConsumedAndCounted() throws Exception, body in order:
1. Seed the claim-check payload:
     ObjectId oid = new ObjectId();
     Document doc = new Document("_id", oid).append("payload", "big-payload").append("sizeBytes", 11L);
     mongoTemplate.insert(doc, "large_payloads");
2. Build two envelopes (use Instant.now() and System.nanoTime()):
     MessageEnvelope inline = new MessageEnvelope("m-inline", "p1", Instant.now(), System.nanoTime(), 3L, null, "abc", null);
     MessageEnvelope claim = new MessageEnvelope("m-claim", "p1", Instant.now(), System.nanoTime(), 11L, null, null, oid.toHexString());
3. Serialize each with `new ObjectMapper().registerModule(new JavaTimeModule())`
   via mapper.writeValueAsString (checked exception propagates through the test's
   `throws Exception` — do NOT try/catch).
4. Build a KafkaProducer<String,String> with EXACTLY this config:
     Map<String, Object> config = new HashMap<>();
     config.put("bootstrap.servers", kafka.getBootstrapServers());
     KafkaProducer<String, String> producer =
         new KafkaProducer<>(config, new StringSerializer(), new StringSerializer());
   In a try/finally (finally: producer.close()):
     producer.send(new ProducerRecord<>("messages", inline JSON)).get();
     producer.send(new ProducerRecord<>("messages", claim JSON)).get();
5. Await consumption: poll in a plain for loop up to 60 iterations, sleeping
   Thread.sleep(500L) between iterations; read
     double inlineCount = registry.counter("consumer.messages", "path", "INLINE").count();
     double claimCount = registry.counter("consumer.messages", "path", "CLAIM_CHECK").count();
   break when both are >= 1.0.
6. assertThat(inlineCount).isGreaterThanOrEqualTo(1.0);
   assertThat(claimCount).isGreaterThanOrEqualTo(1.0);
   assertThat(registry.counter("consumer.bytes", "path", "INLINE").count()).isGreaterThanOrEqualTo(3.0);
   assertThat(registry.counter("consumer.bytes", "path", "CLAIM_CHECK").count()).isGreaterThanOrEqualTo(11.0);

The two count variables must be declared BEFORE the loop (initialized 0.0) so they
are in scope for the assertions.
