Write a Spring @Component class.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: consumer-service/src/main/java/com/codrite/claimcheck/consumer/MongoPayloadReader.java
Package: com.codrite.claimcheck.consumer

ALREADY EXIST in this package (reference, do NOT declare):
- interface PayloadReader { java.util.Optional<String> fetch(String mongoId); }
- class ConsumerMetrics with method `public io.micrometer.core.instrument.Timer mongoFetch()`
- enum DeliveryPath

Output EXACTLY ONE top-level class, max 45 lines:

@Component public final class MongoPayloadReader implements PayloadReader
- Constructor: `public MongoPayloadReader(org.springframework.data.mongodb.core.MongoTemplate template, ConsumerMetrics metrics)`
  storing both in private final fields.
- `fetch(String mongoId)`: start `Timer.Sample sample = Timer.start();` then in a
  try/finally (finally: `sample.stop(metrics.mongoFetch());`):
  - Look up the document by id in collection "large_payloads":
    `org.bson.Document doc = template.findById(new org.bson.types.ObjectId(mongoId), org.bson.Document.class, "large_payloads");`
  - return `doc == null ? java.util.Optional.empty() : java.util.Optional.ofNullable(doc.getString("payload"));`

Imports: org.bson.Document, org.bson.types.ObjectId, org.springframework.data.mongodb.core.MongoTemplate,
org.springframework.stereotype.Component, io.micrometer.core.instrument.Timer, java.util.Optional.
NO wildcard imports. Do not add any other methods.
