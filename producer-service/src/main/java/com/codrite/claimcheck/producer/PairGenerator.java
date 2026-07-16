package com.codrite.claimcheck.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codrite.claimcheck.producer.DeliveryPath;
import java.util.UUID;

@org.springframework.stereotype.Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = "app.load.enabled", havingValue = "true", matchIfMissing = true)
public final class PairGenerator {
    private final ClaimCheckRouter router;
    private final EnvelopePublisher publisher;
    private final int payloadBytes;

    public PairGenerator(ClaimCheckRouter router, EnvelopePublisher publisher,
            @org.springframework.beans.factory.annotation.Value("${app.load.payload-bytes:2097152}") int payloadBytes) {
        this.router = router;
        this.publisher = publisher;
        this.payloadBytes = payloadBytes;
    }

    private static final Logger log = LoggerFactory.getLogger(PairGenerator.class);

    @org.springframework.scheduling.annotation.Scheduled(fixedRateString = "${app.load.tick-millis:200}")
    public void tick() {
        String payload = "x".repeat(payloadBytes);
        String pairId = java.util.UUID.randomUUID().toString();
        publisher.publish(router.route(UUID.randomUUID().toString(), pairId, payload, DeliveryPath.INLINE));
        publisher.publish(router.route(UUID.randomUUID().toString(), pairId, payload, DeliveryPath.CLAIM_CHECK));
        log.info("Generated pairId: {}, payloadBytes: {}", pairId, payloadBytes);
    }
}
