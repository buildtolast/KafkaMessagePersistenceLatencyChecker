package com.codrite.claimcheck.dashboard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public final class MetricsAggregator {
    private final double windowSeconds;
    private Map<String, Double> previousCounters = new HashMap<>();
    private final Set<String> knownInstances = new TreeSet<>();

    public MetricsAggregator(double windowSeconds) {
        this.windowSeconds = windowSeconds;
    }

    public synchronized ComparisonModel aggregate(Map<String, List<MetricSample>> snapshots) {
        knownInstances.addAll(snapshots.keySet());
        Map<String, Double> currentCounters = new HashMap<>();
        for (Map.Entry<String, List<MetricSample>> entry : snapshots.entrySet()) {
            String inst = entry.getKey();
            for (MetricSample sm : entry.getValue()) {
                if (sm.name().endsWith("_total")) {
                    currentCounters.put(inst + "|" + sm.name() + "|" + pathOf(sm), sm.value());
                }
            }
        }

        Map<String, ComparisonModel.PathStats> byPath = new LinkedHashMap<>();
        String[] paths = {"INLINE", "CLAIM_CHECK"};
        for (String p : paths) {
            double msg = getRate(snapshots, currentCounters, "consumer_messages_total", p);
            double mb = getRate(snapshots, currentCounters, "consumer_bytes_total", p) / 1_000_000.0;
            double p50 = getWeightedAvg(snapshots, "consumer_e2e_latency_seconds", "0.5", p);
            double p95 = getWeightedAvg(snapshots, "consumer_e2e_latency_seconds", "0.95", p);
            double p99 = getWeightedAvg(snapshots, "consumer_e2e_latency_seconds", "0.99", p);
            double mIns = getAvgMs(snapshots, "producer_mongo_insert_seconds", p);
            double kSend = getAvgMs(snapshots, "producer_kafka_send_seconds", p);
            double mFetch = getAvgMs(snapshots, "consumer_mongo_fetch_seconds", p);
            double proc = getAvgMs(snapshots, "consumer_processing_seconds", p);
            byPath.put(p, new ComparisonModel.PathStats(msg, mb, p50, p95, p99, mIns, kSend, mFetch, proc));
        }

        long mongoDocs = 0;
        double mongoBytes = 0;
        for (List<MetricSample> samples : snapshots.values()) {
            for (MetricSample sm : samples) {
                if (pathOf(sm).equals("CLAIM_CHECK")) {
                    if (sm.name().equals("producer_messages_total")) mongoDocs += (long) sm.value();
                    if (sm.name().equals("producer_bytes_total")) mongoBytes += sm.value();
                }
            }
        }

        List<ComparisonModel.InstanceHealth> instances = new ArrayList<>();
        for (String inst : knownInstances) {
            boolean up = snapshots.containsKey(inst);
            double instMsg = 0.0;
            if (up) {
                for (String p : paths) {
                    instMsg += getInstRate(inst, snapshots, currentCounters, "consumer_messages_total", p);
                }
            }
            double lag = 0.0;
            if (up) {
                for (MetricSample sm : snapshots.get(inst)) {
                    if (sm.name().equals("kafka_consumer_fetch_manager_records_lag_max")) {
                        lag = sm.value();
                        break;
                    }
                }
            }
            instances.add(new ComparisonModel.InstanceHealth(inst, up, instMsg, lag));
        }

        previousCounters = currentCounters;
        return new ComparisonModel(byPath, instances, mongoDocs, mongoBytes);
    }

    private String pathOf(MetricSample sm) {
        return sm.tags().getOrDefault("path", "");
    }

    private double sumValues(Map<String, List<MetricSample>> snapshots, String name, String path) {
        double sum = 0;
        for (List<MetricSample> list : snapshots.values()) {
            for (MetricSample sm : list) {
                if (sm.name().equals(name) && pathOf(sm).equals(path) && !sm.tags().containsKey("quantile")) {
                    sum += sm.value();
                }
            }
        }
        return sum;
    }

    private double getRate(Map<String, List<MetricSample>> snapshots, Map<String, Double> current, String name, String path) {
        double totalDelta = 0;
        for (Map.Entry<String, List<MetricSample>> entry : snapshots.entrySet()) {
            String inst = entry.getKey();
            String key = inst + "|" + name + "|" + path;
            double curVal = current.getOrDefault(key, 0.0);
            double prevVal = previousCounters.getOrDefault(key, curVal);
            totalDelta += (curVal - prevVal);
        }
        return totalDelta / windowSeconds;
    }

    private double getInstRate(String inst, Map<String, List<MetricSample>> snapshots, Map<String, Double> current, String name, String path) {
        String key = inst + "|" + name + "|" + path;
        double curVal = current.getOrDefault(key, 0.0);
        double prevVal = previousCounters.getOrDefault(key, curVal);
        return (curVal - prevVal) / windowSeconds;
    }

    private double getWeightedAvg(Map<String, List<MetricSample>> snapshots, String baseName, String quantile, String path) {
        double weightedSum = 0;
        double totalWeight = 0;
        for (List<MetricSample> list : snapshots.values()) {
            double weight = 1.0;
            for (MetricSample sm : list) {
                if (sm.name().equals(baseName + "_count") && pathOf(sm).equals(path)) {
                    weight = sm.value();
                    break;
                }
            }
            if (weight <= 0) continue;
            for (MetricSample sm : list) {
                if (sm.name().equals(baseName) && pathOf(sm).equals(path) && quantile.equals(sm.tags().get("quantile"))) {
                    weightedSum += (sm.value() * weight);
                    totalWeight += weight;
                    break;
                }
            }
        }
        return totalWeight == 0 ? 0.0 : (weightedSum / totalWeight) * 1000.0;
    }

    private double getAvgMs(Map<String, List<MetricSample>> snapshots, String baseName, String path) {
        double sum = sumValues(snapshots, baseName + "_sum", path);
        double count = sumValues(snapshots, baseName + "_count", path);
        return count == 0 ? 0.0 : (sum / count) * 1000.0;
    }
}
