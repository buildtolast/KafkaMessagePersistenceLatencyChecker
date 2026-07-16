Write a JUnit 5 test class (plain unit test, NO Spring).

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: dashboard-service/src/test/java/com/codrite/claimcheck/dashboard/MetricsAggregatorTest.java
Package: com.codrite.claimcheck.dashboard

These types are in the SAME package — reference them, do NOT declare them, do NOT
import them:
- record MetricSample(String name, java.util.Map<String,String> tags, double value)
- record ComparisonModel(java.util.Map<String, ComparisonModel.PathStats> byPath,
    java.util.List<ComparisonModel.InstanceHealth> instances, long mongoDocs, double mongoBytes)
  with nested records PathStats(double msgPerSec, double mbPerSec, double e2eP50Ms,
  double e2eP95Ms, double e2eP99Ms, double mongoInsertAvgMs, double kafkaSendAvgMs,
  double mongoFetchAvgMs, double processingAvgMs) and
  InstanceHealth(String name, boolean up, double msgPerSec, double lag)
- final class MetricsAggregator with constructor
  `public MetricsAggregator(double windowSeconds)` and method
  `public synchronized ComparisonModel aggregate(java.util.Map<String, java.util.List<MetricSample>> snapshots)`

Output EXACTLY ONE top-level class MetricsAggregatorTest, max 130 lines.
Imports: org.junit.jupiter.api.Test, static org.junit.jupiter.api.Assertions.*,
java.util.ArrayList, java.util.HashMap, java.util.List, java.util.Map.
NO other imports. NO streams.

Two private static helpers:
- `MetricSample s(String name, String path, double value)` → new MetricSample(name,
  Map.of("path", path), value)
- `MetricSample q(String name, String path, String quantile, double value)` →
  new MetricSample(name, Map.of("path", path, "quantile", quantile), value)

Tests:

1. ratesComeFromCounterDeltasAcrossCalls:
   MetricsAggregator agg = new MetricsAggregator(2.0);
   Map<String, List<MetricSample>> snap1 = new HashMap<>();
   snap1.put("consumer1", List.of(
       s("consumer_messages_total", "INLINE", 10.0),
       s("consumer_bytes_total", "INLINE", 1000000.0)));
   ComparisonModel first = agg.aggregate(snap1);
   assertEquals(0.0, first.byPath().get("INLINE").msgPerSec(), 1e-9);  // no previous call
   Map<String, List<MetricSample>> snap2 = new HashMap<>();
   snap2.put("consumer1", List.of(
       s("consumer_messages_total", "INLINE", 20.0),
       s("consumer_bytes_total", "INLINE", 3000000.0)));
   ComparisonModel second = agg.aggregate(snap2);
   assertEquals(5.0, second.byPath().get("INLINE").msgPerSec(), 1e-9);   // (20-10)/2
   assertEquals(1.0, second.byPath().get("INLINE").mbPerSec(), 1e-9);    // (3e6-1e6)/2/1e6

2. quantilesWeightedByCountAcrossInstances:
   new MetricsAggregator(2.0); one aggregate call with two instances:
   "consumer1": q("consumer_e2e_latency_seconds","INLINE","0.5",0.010) and
                s("consumer_e2e_latency_seconds_count","INLINE",10.0)
   "consumer2": q("consumer_e2e_latency_seconds","INLINE","0.5",0.020) and
                s("consumer_e2e_latency_seconds_count","INLINE",30.0)
   Expect e2eP50Ms = (0.010*10 + 0.020*30)/40 * 1000 = 17.5:
   assertEquals(17.5, model.byPath().get("INLINE").e2eP50Ms(), 1e-6);

3. timerAveragesFromSumOverCount:
   one call, one instance "producer" with:
   s("producer_mongo_insert_seconds_sum","CLAIM_CHECK",0.4),
   s("producer_mongo_insert_seconds_count","CLAIM_CHECK",10.0),
   s("consumer_processing_seconds_sum","INLINE",0.05),
   s("consumer_processing_seconds_count","INLINE",10.0)
   Expect mongoInsertAvgMs for CLAIM_CHECK = 0.4/10*1000 = 40.0 and
   processingAvgMs for INLINE = 5.0.

4. mongoTotalsFromProducerClaimCheckCounters:
   one call, instance "producer" with
   s("producer_messages_total","CLAIM_CHECK",7.0),
   s("producer_bytes_total","CLAIM_CHECK",14000000.0)
   → assertEquals(7L, model.mongoDocs()); assertEquals(14000000.0, model.mongoBytes(), 1e-9).

5. absentSnapshotMarksInstanceDown:
   call 1 with keys "consumer1" and "consumer2" (each just
   s("consumer_messages_total","INLINE",1.0));
   call 2 with only "consumer1".
   In the second model's instances() list, find by name with a plain for loop:
   "consumer1" → up() true; "consumer2" → up() false.

6. perInstanceRatesAreIndependent:
   agg = new MetricsAggregator(2.0);
   call 1: "consumer1" → s("consumer_messages_total","INLINE",10.0);
           "consumer2" → s("consumer_messages_total","INLINE",10.0).
   call 2: "consumer1" → s("consumer_messages_total","INLINE",30.0)   // delta 20 → 10/s
           "consumer2" → s("consumer_messages_total","INLINE",14.0).  // delta 4  → 2/s
   In the second model's instances(), find each by name with a plain for loop and
   assert consumer1 msgPerSec() == 10.0 and consumer2 msgPerSec() == 2.0 (1e-9).
   Each instance's rate MUST reflect only its own counters.

7. missingMetricsYieldZeroStats:
   fresh aggregator, one call with one instance and an empty sample list
   (`new ArrayList<MetricSample>()`); byPath() must still contain keys "INLINE"
   and "CLAIM_CHECK" with all-zero PathStats:
   assertEquals(0.0, model.byPath().get("CLAIM_CHECK").e2eP95Ms(), 1e-9);
   assertEquals(0L, model.mongoDocs());
