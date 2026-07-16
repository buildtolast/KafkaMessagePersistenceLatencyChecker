package com.codrite.claimcheck.consumer;

public record ResolvedMessage(String messageId, DeliveryPath path, String payload, long sizeBytes) {}
