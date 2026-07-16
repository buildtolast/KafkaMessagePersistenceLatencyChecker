Write a Java record.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: dashboard-service/src/main/java/com/codrite/claimcheck/dashboard/MetricSample.java
Package: com.codrite.claimcheck.dashboard

Output EXACTLY this record, max 8 lines:

public record MetricSample(String name, java.util.Map<String, String> tags, double value) {}

Use a single-line import for java.util.Map and declare the component as Map<String, String>.
No methods, no annotations.
