package com.codrite.claimcheck.producer;

import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.Date;

@Component
public final class MongoPayloadStore implements PayloadStore {
    private final MongoTemplate template;
    private final ProducerMetrics metrics;

    public MongoPayloadStore(MongoTemplate template, ProducerMetrics metrics) {
        this.template = template;
        this.metrics = metrics;
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
            sample.stop(metrics.mongoInsert(DeliveryPath.CLAIM_CHECK));
        }
    }
}
