package com.codrite.claimcheck.producer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

@Component
public final class ProducerMetrics {
    private final MeterRegistry registry;

    public ProducerMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public Timer mongoInsert(DeliveryPath p) {
        return Timer.builder("producer.mongo.insert")
                .tag("path", p.name())
                .publishPercentileHistogram()
                .register(registry);
    }

    public Timer kafkaSend(DeliveryPath p) {
        return Timer.builder("producer.kafka.send")
                .tag("path", p.name())
                .publishPercentileHistogram()
                .register(registry);
    }

    public void recordMessage(DeliveryPath p, long bytes) {
        registry.counter("producer.messages", "path", p.name()).increment();
        registry.counter("producer.bytes", "path", p.name()).increment(bytes);
    }
}
