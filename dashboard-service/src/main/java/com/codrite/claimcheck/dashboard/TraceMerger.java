package com.codrite.claimcheck.dashboard;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TraceMerger {

    private TraceMerger() {}

    public static WaterfallModel merge(List<TraceRecord> inlineCandidates, List<TraceRecord> claimCheckCandidates) {
        if (inlineCandidates == null) inlineCandidates = Collections.emptyList();
        if (claimCheckCandidates == null) claimCheckCandidates = Collections.emptyList();

        Map<String, TraceRecord> inlineMap = inlineCandidates.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(TraceRecord::pairId, Function.identity(), (existing, replacement) -> existing));

        Map<String, TraceRecord> claimCheckMap = claimCheckCandidates.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(TraceRecord::pairId, Function.identity(), (existing, replacement) -> existing));

        Set<String> intersection = new HashSet<>(inlineMap.keySet());
        intersection.retainAll(claimCheckMap.keySet());

        if (!intersection.isEmpty()) {
            String bestPairId = null;
            long maxCombinedRecency = Long.MIN_VALUE;

            for (String pairId : intersection) {
                long combined = getRecency(inlineMap.get(pairId)) + getRecency(claimCheckMap.get(pairId));
                if (bestPairId == null || combined > maxCombinedRecency) {
                    maxCombinedRecency = combined;
                    bestPairId = pairId;
                }
            }
            return new WaterfallModel(inlineMap.get(bestPairId), claimCheckMap.get(bestPairId));
        }

        TraceRecord bestInline = findMostRecent(inlineCandidates);
        TraceRecord bestClaimCheck = findMostRecent(claimCheckCandidates);

        return new WaterfallModel(bestInline, bestClaimCheck);
    }

    private static long getRecency(TraceRecord record) {
        if (record == null || record.hopTrace() == null || record.hopTrace().isEmpty()) {
            return Long.MIN_VALUE;
        }
        List<HopRecord> hops = record.hopTrace();
        HopRecord lastHop = hops.get(hops.size() - 1);
        return lastHop.consumedAtEpochNanos();
    }

    private static TraceRecord findMostRecent(List<TraceRecord> candidates) {
        TraceRecord best = null;
        long maxRecency = Long.MIN_VALUE;

        for (TraceRecord record : candidates) {
            if (record == null) continue;
            long recency = getRecency(record);
            if (best == null || recency > maxRecency) {
                maxRecency = recency;
                best = record;
            }
        }
        return best;
    }
}
