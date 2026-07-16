Write a Java interface.

Output path: consumer-service/src/main/java/com/codrite/claimcheck/consumer/PayloadReader.java
Package: com.codrite.claimcheck.consumer

Output EXACTLY this interface, nothing else, exactly ONE fenced code block:

```java
public interface PayloadReader {
  java.util.Optional<String> fetch(String mongoId);
}
```

Add the package declaration. Exactly one top-level type. Max 10 lines.
