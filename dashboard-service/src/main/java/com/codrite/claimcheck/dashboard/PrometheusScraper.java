package com.codrite.claimcheck.dashboard;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PrometheusScraper {

    private static final Pattern LINE_PATTERN = Pattern.compile("^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\\{([^}]*)\\})?\\s+(\\S+)$");

    private PrometheusScraper() {}

    public static List<MetricSample> parse(String body) {
        List<MetricSample> samples = new ArrayList<>();
        if (body == null || body.isEmpty()) {
            return samples;
        }

        String[] lines = body.split("\\R");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }

            Matcher matcher = LINE_PATTERN.matcher(trimmed);
            if (matcher.matches()) {
                String name = matcher.group(1);
                String tagString = matcher.group(2);
                String valueStr = matcher.group(3);

                try {
                    double value = Double.parseDouble(valueStr);
                    if (Double.isNaN(value)) {
                        continue;
                    }

                    Map<String, String> tags = Map.of();
                    if (tagString != null && !tagString.isEmpty()) {
                        Map<String, String> tagMap = new LinkedHashMap<>();
                        String[] pairs = tagString.split(",");
                        for (String pair : pairs) {
                            int eqIdx = pair.indexOf('=');
                            if (eqIdx != -1) {
                                String key = pair.substring(0, eqIdx).trim();
                                String val = pair.substring(eqIdx + 1).trim();
                                if (val.startsWith("\"") && val.endsWith("\"") && val.length() >= 2) {
                                    val = val.substring(1, val.length() - 1);
                                }
                                tagMap.put(key, val);
                            }
                        }
                        tags = Map.copyOf(tagMap);
                    }

                    samples.add(new MetricSample(name, tags, value));
                } catch (NumberFormatException e) {
                    // Skip invalid numeric values
                }
            }
        }
        return samples;
    }
}
