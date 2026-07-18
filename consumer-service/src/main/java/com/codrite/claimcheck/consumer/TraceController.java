package com.codrite.claimcheck.consumer;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@RestController
public class TraceController {

    private final TraceBuffer buffer;

    public TraceController(TraceBuffer buffer) {
        this.buffer = buffer;
    }

    @GetMapping("/api/traces/latest")
    public Map<String, TraceRecord> latestTraces() {
        Map<String, TraceRecord> traces = new LinkedHashMap<>();
        for (DeliveryPath path : DeliveryPath.values()) {
            Optional<TraceRecord> record = buffer.latest(path);
            if (record.isPresent()) {
                traces.put(path.name(), record.get());
            }
        }
        return traces;
    }
}
