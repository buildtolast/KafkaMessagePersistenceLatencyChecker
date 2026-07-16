Write a Java record with two nested records.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: dashboard-service/src/main/java/com/codrite/claimcheck/dashboard/ComparisonModel.java
Package: com.codrite.claimcheck.dashboard

Output EXACTLY this content (one top-level record, two nested records), max 20 lines:

public record ComparisonModel(
    java.util.Map<String, PathStats> byPath,
    java.util.List<InstanceHealth> instances,
    long mongoDocs,
    double mongoBytes) {

  public record PathStats(double msgPerSec, double mbPerSec,
      double e2eP50Ms, double e2eP95Ms, double e2eP99Ms,
      double mongoInsertAvgMs, double kafkaSendAvgMs,
      double mongoFetchAvgMs, double processingAvgMs) {}

  public record InstanceHealth(String name, boolean up, double msgPerSec, double lag) {}
}

Use single-class imports for java.util.Map and java.util.List and drop the
java.util. prefixes in the declarations. No methods, no annotations.
