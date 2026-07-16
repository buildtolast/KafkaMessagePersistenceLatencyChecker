package com.codrite.claimcheck.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.beans.factory.annotation.Value;
import com.codrite.claimcheck.producer.PayloadStore;
import com.codrite.claimcheck.producer.ClaimCheckRouter;

@SpringBootApplication
@EnableScheduling
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @Bean
    public ClaimCheckRouter claimCheckRouter(
            @Value("${app.claim-check.threshold-bytes:2097152}") long threshold,
            PayloadStore store) {
        return new ClaimCheckRouter(threshold, store);
    }
}
