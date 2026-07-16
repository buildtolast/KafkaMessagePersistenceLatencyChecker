Write a JUnit 5 Spring Boot integration test using Testcontainers.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text, NO explanation, and NO additional code blocks
before or after it. A reply containing more than one ``` fence is INVALID and
will corrupt the build.

Output path: producer-service/src/test/java/com/codrite/claimcheck/producer/ProducerIntegrationTest.java
Package: com.codrite.claimcheck.producer

ALREADY EXIST in this package (do NOT declare any of them): DeliveryPath enum
{ INLINE, CLAIM_CHECK }; MessageEnvelope record with accessors messageId(),
pairId(), payload(), mongoId(); ClaimCheckRouter; PayloadStore; ProducerApplication
(the @SpringBootApplication class); PairGenerator (scheduled, sends one INLINE and
one CLAIM_CHECK envelope per tick sharing a pairId).

Output EXACTLY ONE top-level class ProducerIntegrationTest. ONE fenced code block,
max 130 lines.

IMPORT RULES: begin the file with EXACTLY this import block, copied verbatim,
adding only java.util.* single-class imports you additionally need:

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Duration;
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

Note "import static" on the first line — it is a static import; plain
`import org.assertj.core.api.Assertions.assertThat` does not compile.
- NO wildcard imports. NO other assertion classes.
- NO method references to methods that throw checked exceptions
  (mapper::readValue is FORBIDDEN — deserialize inside a plain for loop with
  the checked exception propagating from the test method's `throws Exception`).
- NO java streams for grouping; group pairs with a HashMap<String,List<MessageEnvelope>>
  built in a for loop.
- Do not import classes from the test's own package (same-package types need no import).
- Do not assume list ordering for the pair: identify the inline envelope as the one
  with payload() != null and the claim-check one as the one with mongoId() != null,
  regardless of position.

Class annotations:
@org.testcontainers.junit.jupiter.Testcontainers
@org.springframework.boot.test.context.SpringBootTest
Static containers:
@org.testcontainers.junit.jupiter.Container static org.testcontainers.kafka.KafkaContainer kafka =
  new org.testcontainers.kafka.KafkaContainer("apache/kafka:3.9.1");
@org.testcontainers.junit.jupiter.Container static org.testcontainers.containers.MongoDBContainer mongo =
  new org.testcontainers.containers.MongoDBContainer("mongo:7");

@org.springframework.test.context.DynamicPropertySource static void props(DynamicPropertyRegistry r):
  r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  r.add("spring.data.mongodb.uri", () -> mongo.getReplicaSetUrl() + "?directConnection=true");
  (the directConnection parameter is REQUIRED — replica-set discovery breaks port mapping)
  r.add("app.load.payload-bytes", () -> 64);
  r.add("app.load.tick-millis", () -> 200);

Autowire org.springframework.data.mongodb.core.MongoTemplate.

One @Test pairIsSplitAcrossBothPaths() throws Exception:
- Build a KafkaConsumer<String,String> with EXACTLY this config (all three keys are
  MANDATORY — the consumer throws without bootstrap.servers and group.id):
      Map<String, Object> config = new HashMap<>();
      config.put("bootstrap.servers", kafka.getBootstrapServers());
      config.put("group.id", "it-test");
      config.put("auto.offset.reset", "earliest");
      KafkaConsumer<String, String> consumer =
          new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer());
  Then subscribe to "messages".
- Poll in a loop (java.time.Duration.ofMillis(500)) up to 30 seconds. Deserialize
  every record value inside the loop and group into
  Map<String,List<MessageEnvelope>> by pairId(). Keep polling until SOME pairId
  has BOTH its envelopes (a list of size 2) — do NOT stop at "2 records total":
  the first two records can belong to two different pairs, which previously
  broke this test with a NullPointerException. NOTE:
  poll() returns ConsumerRecords which is Iterable but NOT a Collection — never
  pass it to addAll; iterate it with an enhanced for loop
  (`for (org.apache.kafka.clients.consumer.ConsumerRecord<String,String> rec : consumer.poll(...))`)
  and add each record's value to the list.
- Deserialize each value with a Jackson ObjectMapper registered with
  com.fasterxml.jackson.datatype.jsr310.JavaTimeModule into MessageEnvelope.
- Find two envelopes sharing the same pairId() (group by pairId, pick a pair with 2).
- assertThat exactly one of the two has payload() != null and mongoId() == null,
  and the other has mongoId() != null and payload() == null (org.assertj).
- For the claim-check one: mongoTemplate.getCollection("large_payloads") — instead
  simpler: assertThat(mongoTemplate.count(new org.springframework.data.mongodb.core.query.Query(), "large_payloads")).isGreaterThan(0).
- Close the consumer in a finally block.