Write the dashboard Spring Boot main class.

ABSOLUTE OUTPUT RULE: your ENTIRE reply is ONE ```java fenced code block containing
the complete file, with NO text and NO additional code blocks before or after it.

Output path: dashboard-service/src/main/java/com/codrite/claimcheck/dashboard/DashboardApplication.java
Package: com.codrite.claimcheck.dashboard

Output EXACTLY ONE top-level class, max 15 lines:

@SpringBootApplication
@EnableScheduling
public class DashboardApplication
- `public static void main(String[] args)` calling `SpringApplication.run(DashboardApplication.class, args);`

Imports (no wildcard imports): org.springframework.boot.SpringApplication,
org.springframework.boot.autoconfigure.SpringBootApplication,
org.springframework.scheduling.annotation.EnableScheduling.
