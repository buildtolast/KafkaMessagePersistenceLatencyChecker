package com.codrite.claimcheck.dashboard;

public record HopRecord(int stage, long consumedAtEpochNanos, Long publishedAtEpochNanos) {
}
