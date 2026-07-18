package com.codrite.claimcheck.dashboard;

import org.junit.jupiter.api.Test;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TraceMergerTest {

    private static TraceRecord trace(String pairId, String path, long lastHopConsumedAtEpochNanos) {
        return new TraceRecord(pairId, path, List.of(new HopRecord(1, lastHopConsumedAtEpochNanos, null)));
    }

    @Test
    void bothListsEmptyProducesNullBothFields() {
        WaterfallModel result = TraceMerger.merge(List.of(), List.of());
        assertThat(result.inline()).isNull();
        assertThat(result.claimCheck()).isNull();
    }

    @Test
    void matchedPairByPairIdIsPreferredOverIndependentNewest() {
        // "shared-pair" is a match, but individually older than "other-pair"
        TraceRecord inlineShared = trace("shared-pair", "/path1", 100L);
        TraceRecord inlineNewest = trace("other-pair", "/path2", 999L);
        TraceRecord claimCheckShared = trace("shared-pair", "/path3", 200L);

        WaterfallModel result = TraceMerger.merge(
            List.of(inlineShared, inlineNewest),
            List.of(claimCheckShared)
        );

        assertThat(result.inline()).isNotNull();
        assertThat(result.inline().pairId()).isEqualTo("shared-pair");
        assertThat(result.claimCheck()).isNotNull();
        assertThat(result.claimCheck().pairId()).isEqualTo("shared-pair");
    }

    @Test
    void noSharedPairIdFallsBackToIndependentNewestPerList() {
        // Inline candidates: 100 and 500. Newest is 500.
        TraceRecord inline1 = trace("inline-1", "/p1", 100L);
        TraceRecord inline2 = trace("inline-2", "/p2", 500L);

        // ClaimCheck candidates: 50 and 300. Newest is 300.
        TraceRecord claim1 = trace("claim-1", "/p3", 50L);
        TraceRecord claim2 = trace("claim-2", "/p4", 300L);

        WaterfallModel result = TraceMerger.merge(
            List.of(inline1, inline2),
            List.of(claim1, claim2)
        );

        assertThat(result.inline().pairId()).isEqualTo("inline-2");
        assertThat(result.claimCheck().pairId()).isEqualTo("claim-2");
    }

    @Test
    void oneListEmptyStillReturnsNewestFromTheOther() {
        TraceRecord claimCheck = trace("claim-1", "/p1", 1000L);

        WaterfallModel result = TraceMerger.merge(
            List.of(),
            List.of(claimCheck)
        );

        assertThat(result.inline()).isNull();
        assertThat(result.claimCheck()).isNotNull();
        assertThat(result.claimCheck().pairId()).isEqualTo("claim-1");
    }

    @Test
    void multipleMatchedPairsPicksHighestCombinedRecency() {
        // Pair A: 100 + 100 = 200
        TraceRecord inlineA = trace("pair-a", "/p1", 100L);
        TraceRecord claimA = trace("pair-a", "/p2", 100L);

        // Pair B: 400 + 400 = 800
        TraceRecord inlineB = trace("pair-b", "/p3", 400L);
        TraceRecord claimB = trace("pair-b", "/p4", 400L);

        WaterfallModel result = TraceMerger.merge(
            List.of(inlineA, inlineB),
            List.of(claimA, claimB)
        );

        assertThat(result.inline().pairId()).isEqualTo("pair-b");
        assertThat(result.claimCheck().pairId()).isEqualTo("pair-b");
    }
}
