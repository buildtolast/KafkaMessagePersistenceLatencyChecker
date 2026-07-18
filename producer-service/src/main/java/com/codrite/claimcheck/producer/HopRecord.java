package com.codrite.claimcheck.producer;

public record HopRecord(int stage, long consumedAtEpochNanos, Long publishedAtEpochNanos) {
}
