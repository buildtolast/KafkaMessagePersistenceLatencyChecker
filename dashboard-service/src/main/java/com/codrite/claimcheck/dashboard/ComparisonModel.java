package com.codrite.claimcheck.dashboard;

import java.util.Map;
import java.util.List;

public record ComparisonModel(
    Map<String, PathStats> byPath,
    List<InstanceHealth> instances,
    long mongoDocs,
    double mongoBytes,
    WaterfallModel waterfall) {

  public record PathStats(double msgPerSec, double mbPerSec,
      double e2eP50Ms, double e2eP95Ms, double e2eP99Ms,
      double mongoInsertAvgMs, double kafkaSendAvgMs,
      double mongoFetchAvgMs, Map<Integer, Double> hopLatencyAvgMsByStage) {}

  public record InstanceHealth(String name, boolean up, double msgPerSec, double lag) {}
}
