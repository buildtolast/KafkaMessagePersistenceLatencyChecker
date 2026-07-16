Write a Spring Boot application class.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: consumer-service/src/main/java/com/codrite/claimcheck/consumer/ConsumerApplication.java
Package: com.codrite.claimcheck.consumer

ALREADY EXIST in this package (reference, do NOT declare):
- interface PayloadReader
- final class ClaimCheckResolver with constructor `public ClaimCheckResolver(PayloadReader reader)`

Output EXACTLY ONE top-level class, max 25 lines:

@SpringBootApplication
public class ConsumerApplication {
  public static void main(String[] args) { SpringApplication.run(ConsumerApplication.class, args); }

  @Bean
  public ClaimCheckResolver claimCheckResolver(PayloadReader reader) {
    return new ClaimCheckResolver(reader);
  }
}

Imports: org.springframework.boot.SpringApplication,
org.springframework.boot.autoconfigure.SpringBootApplication,
org.springframework.context.annotation.Bean. NO wildcard imports, nothing else.
