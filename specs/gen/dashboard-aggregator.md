Write a metrics aggregation class.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: dashboard-service/src/main/java/com/codrite/claimcheck/dashboard/MetricsAggregator.java
Package: com.codrite.claimcheck.dashboard

ALREADY EXIST in this package (reference, do NOT declare):
- record MetricSample(String name, java.util.Map<String,String> tags, double value)
- record ComparisonModel(java.util.Map<String, ComparisonModel.PathStats> byPath,
    java.util.List<ComparisonModel.InstanceHealth> instances, long mongoDocs, double mongoBytes)
  nested: PathStats(double msgPerSec, double mbPerSec, double e2eP50Ms, double e2eP95Ms,
  double e2eP99Ms, double mongoInsertAvgMs, double kafkaSendAvgMs, double mongoFetchAvgMs,
  double processingAvgMs); InstanceHealth(String name, boolean up, double msgPerSec, double lag)

Output EXACTLY ONE top-level class, max 145 lines. NO wildcard imports. NO streams —
plain for loops only. Allowed imports: java.util.{ArrayList,HashMap,HashSet,
LinkedHashMap,List,Map,Set,TreeSet}.

public final class MetricsAggregator
Fields:
- private final double windowSeconds;
- private Map<String, Double> previousCounters = new HashMap<>();  // key: instance|name|path
- private final Set<String> knownInstances = new TreeSet<>();
Constructor: public MetricsAggregator(double windowSeconds).

public synchronized ComparisonModel aggregate(Map<String, List<MetricSample>> snapshots)

Private helper suggestions (keep code flat and readable):
- String pathOf(MetricSample sm) → sm.tags().getOrDefault("path", "")
- double sumValues(snapshots, String name, String path): loop all instances' samples,
  add value where name and path match AND tags has no "quantile" key.

Algorithm:
1. knownInstances.addAll(snapshots.keySet()).
2. Build Map<String,Double> currentCounters: for every instance entry and every
   sample whose name ends with "_total", put key instance + "|" + name + "|" + pathOf.
3. For each of the two paths "INLINE" and "CLAIM_CHECK" build a PathStats:
   - msgPerSec: sum over currentCounters entries with name "consumer_messages_total"
     and this path of (current - previous.getOrDefault(key, current)) / windowSeconds.
     NOTE: default previous to CURRENT (not 0) so the first call yields 0.0 rate.
   - mbPerSec: same delta logic for "consumer_bytes_total", divided additionally by 1_000_000.0.
   - e2eP50Ms/e2eP95Ms/e2eP99Ms: count-weighted average across instances of the
     samples named "consumer_e2e_latency_seconds" carrying tag quantile equal to
     "0.5"/"0.95"/"0.99" and this path. Weight of an instance = value of its
     "consumer_e2e_latency_seconds_count" sample for this path (default 1.0 when the
     count sample is missing or zero). Result = weightedSum/totalWeight*1000.0, or
     0.0 when no quantile samples exist.
   - mongoInsertAvgMs: sumValues("producer_mongo_insert_seconds_sum", path) /
     sumValues("producer_mongo_insert_seconds_count", path) * 1000.0, or 0.0 when
     the count sum is 0 (guard division by zero).
   - kafkaSendAvgMs: same with "producer_kafka_send_seconds_sum"/"_count".
   - mongoFetchAvgMs: same with "consumer_mongo_fetch_seconds_sum"/"_count".
   - processingAvgMs: same with "consumer_processing_seconds_sum"/"_count".
4. mongoDocs = (long) sum over all instances of "producer_messages_total" samples
   with path CLAIM_CHECK (current totals, NOT deltas).
   mongoBytes = same sum for "producer_bytes_total" with path CLAIM_CHECK.
5. instances: for every name in knownInstances (sorted): up = snapshots.containsKey(name);
   msgPerSec = sum over BOTH paths of THAT SINGLE instance's "consumer_messages_total"
   delta (same previous-defaults-to-current rule) / windowSeconds, 0.0 when down.
   CRITICAL: this is per-instance — compute the delta ONLY from keys
   `name + "|consumer_messages_total|" + path` for THIS instance name; do NOT sum
   across all instances (a dedicated helper taking the instance name is cleanest);
   lag = value of that instance's sample named
   "kafka_consumer_fetch_manager_records_lag_max" (any tags) if present, else 0.0.
6. previousCounters = currentCounters (replace the map reference).
7. Return new ComparisonModel with a LinkedHashMap byPath containing "INLINE" then
   "CLAIM_CHECK".

Both paths must ALWAYS be present in byPath, all-zero when no data.
