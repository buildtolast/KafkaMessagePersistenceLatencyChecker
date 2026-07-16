Write a Java class.
Output path: producer-service/src/main/java/com/codrite/claimcheck/producer/ProducerApplication.java
Package: com.codrite.claimcheck.producer
ALREADY EXIST (do NOT declare): ClaimCheckRouter with constructor
ClaimCheckRouter(long thresholdBytes, PayloadStore store); PayloadStore interface;
MongoPayloadStore (a @Component implementing PayloadStore).
Output EXACTLY ONE top-level class, ONE fenced code block, max 30 lines.

@org.springframework.boot.autoconfigure.SpringBootApplication
@org.springframework.scheduling.annotation.EnableScheduling
public class ProducerApplication {
  public static void main(String[] args) // SpringApplication.run
  @org.springframework.context.annotation.Bean
  public ClaimCheckRouter claimCheckRouter(
      @org.springframework.beans.factory.annotation.Value("${app.claim-check.threshold-bytes:2097152}") long threshold,
      PayloadStore store) // return new ClaimCheckRouter(threshold, store)
}
