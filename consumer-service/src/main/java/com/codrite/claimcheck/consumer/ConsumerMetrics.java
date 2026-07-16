package com.codrite.claimcheck.consumer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class ConsumerMetrics {

    private final MeterRegistry registry;

    public ConsumerMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public Timer e2eLatency(DeliveryPath p) {
        return Timer.builder("consumer.e2e.latency")
                .tag("path", p.name())
                .publishPercentileHistogram()
                .register(registry);
    }

    public Timer processing(DeliveryPath p) {
        return Timer.builder("consumer.processing")
                .tag("path", p.name())
                .publishPercentileHistogram()
                .register(registry);
    }

    public Timer mongoFetch() {
        return Timer.builder("consumer.mongo.fetch")
                .tag("path", DeliveryPath.CLAIM_CHECK.name())
                .publishPercentileHistogram()
                .register(registry);
    }

    public void recordE2e(DeliveryPath p, Duration d) {
        e2eLatency(p).record(d);
    }

    public void recordProcessing(DeliveryPath p, Duration d) {
        processing(p).record(d);
    }

    public void recordMessage(DeliveryPath p, long bytes) {
        registry.counter("consumer.messages", "path", p.name()).increment();
        registry.counter("consumer.bytes", "path", p.name()).increment(bytes);
    }
}
