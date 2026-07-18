package com.codrite.claimcheck.consumer;

import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.Date;

@Component
final class MongoPayloadStore implements PayloadStore {
    private final MongoTemplate template;
    private final Timer insertTimer;

    public MongoPayloadStore(MongoTemplate template, MeterRegistry meterRegistry) {
        this.template = template;
        this.insertTimer = Timer.builder("consumer.mongo.insert")
                .tag("path", "CLAIM_CHECK")
                .register(meterRegistry);
    }

    @Override
    public String store(String payload, long sizeBytes) {
        Timer.Sample sample = Timer.start();
        try {
            Document doc = new Document("payload", payload)
                    .append("sizeBytes", sizeBytes)
                    .append("createdAt", Date.from(Instant.now()));
            template.insert(doc, "large_payloads");
            return doc.getObjectId("_id").toHexString();
        } finally {
            sample.stop(insertTimer);
        }
    }
}
