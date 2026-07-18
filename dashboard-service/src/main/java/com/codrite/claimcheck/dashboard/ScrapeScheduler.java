package com.codrite.claimcheck.dashboard;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class ScrapeScheduler {
    private static final Logger log = LoggerFactory.getLogger(ScrapeScheduler.class);

    private final List<String> targets;
    private final List<String> consumerTraceTargets;
    private final MetricsAggregator aggregator = new MetricsAggregator(2.0);
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(1))
            .build();
    private final AtomicReference<ComparisonModel> latest = new AtomicReference<>(
            new ComparisonModel(Map.of(), List.of(), 0L, 0.0, new WaterfallModel(null, null)));

    public ScrapeScheduler(
            @Value("${app.dashboard.targets}") String targetsCsv,
            @Value("${app.dashboard.consumer-targets:}") String consumerTargetsCsv) {
        this.targets = splitCsv(targetsCsv);
        if (this.targets.isEmpty()) {
            throw new IllegalStateException("app.dashboard.targets is empty");
        }
        // Trace fan-out excludes producer-service, which has no /api/traces/latest
        // endpoint; falls back to app.dashboard.targets if the dedicated
        // consumer-targets property isn't set (e.g. in unit/integration tests).
        this.consumerTraceTargets = consumerTargetsCsv.isBlank() ? this.targets : splitCsv(consumerTargetsCsv);
    }

    private static List<String> splitCsv(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
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

                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    snapshots.put(target, PrometheusScraper.parse(response.body()));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.debug("scrape failed for {}: {}", target, e.toString());
            }
        }
        ComparisonModel base = aggregator.aggregate(snapshots);
        WaterfallModel waterfall = fetchAndMergeTraces();
        latest.set(new ComparisonModel(base.byPath(), base.instances(), base.mongoDocs(), base.mongoBytes(), waterfall));
    }

    private WaterfallModel fetchAndMergeTraces() {
        List<TraceRecord> inlineCandidates = new ArrayList<>();
        List<TraceRecord> claimCheckCandidates = new ArrayList<>();
        for (String target : consumerTraceTargets) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(target + "/api/traces/latest"))
                        .timeout(Duration.ofMillis(1500))
                        .GET()
                        .build();
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    continue;
                }
                Map<String, TraceRecord> traces = mapper.readValue(response.body(),
                        mapper.getTypeFactory().constructMapType(Map.class, String.class, TraceRecord.class));
                TraceRecord inline = traces.get("INLINE");
                if (inline != null) inlineCandidates.add(inline);
                TraceRecord claimCheck = traces.get("CLAIM_CHECK");
                if (claimCheck != null) claimCheckCandidates.add(claimCheck);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new WaterfallModel(null, null);
            } catch (Exception e) {
                log.debug("trace fetch failed for {}: {}", target, e.toString());
            }
        }
        return TraceMerger.merge(inlineCandidates, claimCheckCandidates);
    }

    public ComparisonModel latest() {
        return latest.get();
    }
}
