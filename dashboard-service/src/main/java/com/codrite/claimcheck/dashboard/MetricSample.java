package com.codrite.claimcheck.dashboard;

import java.util.Map;

public record MetricSample(String name, Map<String, String> tags, double value) {}
