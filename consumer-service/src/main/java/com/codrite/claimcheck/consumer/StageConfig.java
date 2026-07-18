package com.codrite.claimcheck.consumer;

public record StageConfig(int stageNumber, int chainLength) {

    public boolean isTerminal() {
        return stageNumber >= chainLength;
    }

    public String nextTopic() {
        if (isTerminal()) {
            return null;
        }
        return String.format("topic-%02d", stageNumber + 1);
    }
}
