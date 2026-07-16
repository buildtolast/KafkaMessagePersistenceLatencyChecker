Write a Prometheus text-exposition parser.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: dashboard-service/src/main/java/com/codrite/claimcheck/dashboard/PrometheusScraper.java
Package: com.codrite.claimcheck.dashboard

ALREADY EXISTS in this package (reference, do NOT declare):
record MetricSample(String name, java.util.Map<String,String> tags, double value)

Output EXACTLY ONE top-level class, max 60 lines:

public final class PrometheusScraper
- private constructor (utility class).
- ONE public static method: `public static java.util.List<MetricSample> parse(String body)`

Behavior:
- Split body into lines. For each trimmed line: skip empty lines and lines
  starting with '#'.
- Each data line is `name{key="v",key2="v2"} value` or `name value`.
  Parse with java.util.regex: one compiled private static final Pattern such as
  `^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)$`.
- Parse the value with Double.parseDouble; if the parse throws
  NumberFormatException OR the value is NaN (Double.isNaN), skip the line.
- Tags: when the brace group is present and non-empty, split on ',' and each
  entry on the FIRST '=' ; strip surrounding double quotes from the value.
  Collect into a java.util.LinkedHashMap. Untagged lines get Map.of().
- Wrap the tags with java.util.Map.copyOf before constructing MetricSample.
- Return a java.util.ArrayList of samples in input order.

NO wildcard imports. NO streams. Plain for loops.
