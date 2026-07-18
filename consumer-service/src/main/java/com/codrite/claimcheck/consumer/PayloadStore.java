package com.codrite.claimcheck.consumer;

public interface PayloadStore {
    String store(String payload, long sizeBytes);
}
