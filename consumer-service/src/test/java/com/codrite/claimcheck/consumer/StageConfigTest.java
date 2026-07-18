package com.codrite.claimcheck.consumer;

import org.junit.jupiter.api.Test;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class StageConfigTest {

    @Test
    void isTerminalTrueWhenStageEqualsChainLength() {
        StageConfig config = new StageConfig(3, 3);
        assertThat(config.isTerminal()).isTrue();
    }

    @Test
    void isTerminalTrueWhenStageExceedsChainLength() {
        StageConfig config = new StageConfig(5, 3);
        assertThat(config.isTerminal()).isTrue();
    }

    @Test
    void isTerminalFalseWhenStageBelowChainLength() {
        StageConfig config = new StageConfig(2, 3);
        assertThat(config.isTerminal()).isFalse();
    }

    @Test
    void nextTopicReturnsPresentZeroPaddedNameWhenNotTerminal() {
        StageConfig config1 = new StageConfig(1, 20);
        Optional<String> topic1 = config1.nextTopic();
        assertThat(topic1).isPresent().contains("topic-02");

        StageConfig config2 = new StageConfig(9, 20);
        Optional<String> topic2 = config2.nextTopic();
        assertThat(topic2).isPresent().contains("topic-10");
    }

    @Test
    void nextTopicReturnsEmptyWhenTerminal() {
        StageConfig config = new StageConfig(3, 3);
        assertThat(config.nextTopic()).isEmpty();
    }
}
