package com.codrite.claimcheck.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ConsumerApplication {

  public static void main(String[] args) {
    SpringApplication.run(ConsumerApplication.class, args);
  }

  @Bean
  public ClaimCheckResolver claimCheckResolver(PayloadReader reader) {
    return new ClaimCheckResolver(reader);
  }
}
