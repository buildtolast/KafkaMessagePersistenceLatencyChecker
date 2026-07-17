package com.codrite.claimcheck.dashboard;

import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

@Component
public class ScrapeScheduler {
    private final List<String> targets;
    private final MetricsAggregator aggregator = new MetricsAggregator(2.0);
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(1))
            .build();
    private final AtomicReference<ComparisonModel> latest = new AtomicReference<>(
            new ComparisonModel(Map.of(), List.of(), 0L, 0.0));

    public ScrapeScheduler(@Value("${app.dashboard.targets}") String targetsCsv) {
        this.targets = Stream.of(targetsCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    @Scheduled(fixedRate = 2000)
    public void scrape() {
        Map<String, List<MetricSample>> snapshots = new HashMap<>();
        for (String target : targets) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(target + "/actuator/prometheus"))
                        .timeout(Duration.ofMillis(1500))
                        .GET()
                        .build();
                HttpResponse<String> response = http.send(request, 
                        HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    snapshots.put(target, PrometheusScraper.parse(response.body()));
                }
            } catch (Exception ignored) {
            }
        }
        latest.set(aggregator.aggregate(snapshots));
    }

    public ComparisonModel latest() {
        return latest.get();
    }
}
