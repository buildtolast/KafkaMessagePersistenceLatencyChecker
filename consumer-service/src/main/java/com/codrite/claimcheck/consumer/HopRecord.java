package com.codrite.claimcheck.consumer;

public record HopRecord(int stage, long consumedAtEpochNanos, Long publishedAtEpochNanos) {
}
