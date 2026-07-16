package com.codrite.claimcheck.producer;

public interface PayloadStore {
    String store(String payload, long sizeBytes);
}
