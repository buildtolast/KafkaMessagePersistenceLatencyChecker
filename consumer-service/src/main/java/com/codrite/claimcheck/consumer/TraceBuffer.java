package com.codrite.claimcheck.consumer;

import org.springframework.stereotype.Component;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public final class TraceBuffer {

    private final ConcurrentHashMap<DeliveryPath, TraceRecord> buffer = new ConcurrentHashMap<>();

    public void record(TraceRecord trace) {
        buffer.put(trace.path(), trace);
    }

    public Optional<TraceRecord> latest(DeliveryPath path) {
        return Optional.ofNullable(buffer.get(path));
    }
}
