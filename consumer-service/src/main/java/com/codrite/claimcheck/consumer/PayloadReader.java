package com.codrite.claimcheck.consumer;

public interface PayloadReader {
  java.util.Optional<String> fetch(String mongoId, int stage);
}
