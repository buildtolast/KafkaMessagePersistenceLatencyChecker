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
        return Timer.builder("chain.e2e.latency")
                .tag("path", p.name())
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    public Timer mongoFetch() {
        return Timer.builder("consumer.mongo.fetch")
                .tag("path", DeliveryPath.CLAIM_CHECK.name())
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    public Timer stageHopLatency(int stage, DeliveryPath p) {
        return Timer.builder("stage.hop.latency")
                .tag("stage", String.valueOf(stage))
                .tag("path", p.name())
                .register(registry);
    }

    public void recordE2e(DeliveryPath p, Duration d) {
        e2eLatency(p).record(d);
    }

    public void recordStageHop(int stage, DeliveryPath p, Duration d) {
        stageHopLatency(stage, p).record(d);
    }

    public void recordMessage(DeliveryPath p, long bytes) {
        registry.counter("consumer.messages", "path", p.name()).increment();
        registry.counter("consumer.bytes", "path", p.name()).increment(bytes);
    }
}
