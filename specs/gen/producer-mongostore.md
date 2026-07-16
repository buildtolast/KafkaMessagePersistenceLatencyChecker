Write a Java class.
Output path: producer-service/src/main/java/com/codrite/claimcheck/producer/MongoPayloadStore.java
Package: com.codrite.claimcheck.producer
ALREADY EXIST (do NOT declare): DeliveryPath enum; PayloadStore interface
{ String store(String payload, long sizeBytes); }; ProducerMetrics class with
method io.micrometer.core.instrument.Timer mongoInsert(DeliveryPath p).
Output EXACTLY ONE top-level class, ONE fenced code block, max 45 lines.

@org.springframework.stereotype.Component
public final class MongoPayloadStore implements PayloadStore {
  public MongoPayloadStore(org.springframework.data.mongodb.core.MongoTemplate template, ProducerMetrics metrics)
  @Override public String store(String payload, long sizeBytes)
}

store(): time the insert with metrics.mongoInsert(DeliveryPath.CLAIM_CHECK).record(Runnable or use Timer.Sample);
insert into collection "large_payloads" an org.bson.Document with fields
"payload" (payload), "sizeBytes" (sizeBytes), "createdAt" (java.util.Date from Instant.now())
using template.insert(doc, "large_payloads"); return the generated ObjectId hex string:
doc.getObjectId("_id").toHexString(). Simplest timing: Timer.Sample sample =
io.micrometer.core.instrument.Timer.start(); ... sample.stop(metrics.mongoInsert(...)).
