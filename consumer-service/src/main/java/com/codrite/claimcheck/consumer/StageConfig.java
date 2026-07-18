package com.codrite.claimcheck.consumer;

import java.util.Optional;

public record StageConfig(int stageNumber, int chainLength) {

    public boolean isTerminal() {
        return stageNumber >= chainLength;
    }

    public Optional<String> nextTopic() {
        if (isTerminal()) {
            return Optional.empty();
        }
        return Optional.of(String.format("topic-%02d", stageNumber + 1));
    }
}
