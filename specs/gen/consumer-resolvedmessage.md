Write a Java record.

Output path: consumer-service/src/main/java/com/codrite/claimcheck/consumer/ResolvedMessage.java
Package: com.codrite.claimcheck.consumer

DeliveryPath ALREADY EXISTS in this package — reference it, do NOT declare it.

Output EXACTLY this record, with the package declaration, exactly ONE fenced
code block, exactly one top-level type, max 10 lines:

```java
public record ResolvedMessage(String messageId, DeliveryPath path, String payload, long sizeBytes) {}
```
