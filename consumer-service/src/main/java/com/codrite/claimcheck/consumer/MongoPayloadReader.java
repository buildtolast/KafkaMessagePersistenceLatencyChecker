package com.codrite.claimcheck.consumer;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.Timer;
import java.util.Optional;

@Component
public final class MongoPayloadReader implements PayloadReader {

    private final MongoTemplate template;
    private final ConsumerMetrics metrics;

    public MongoPayloadReader(MongoTemplate template, ConsumerMetrics metrics) {
        this.template = template;
        this.metrics = metrics;
    }

    @Override
    public Optional<String> fetch(String mongoId) {
        Timer.Sample sample = Timer.start();
        try {
            Document doc = template.findById(new ObjectId(mongoId), Document.class, "large_payloads");
            return doc == null ? Optional.empty() : Optional.ofNullable(doc.getString("payload"));
        } finally {
            sample.stop(metrics.mongoFetch());
        }
    }
}
