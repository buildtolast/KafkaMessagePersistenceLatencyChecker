package com.codrite.claimcheck.consumer;

import java.util.Optional;

public final class ClaimCheckResolver {
    private final PayloadReader reader;

    public ClaimCheckResolver(PayloadReader reader) {
        this.reader = reader;
    }

    public Optional<ResolvedMessage> resolve(MessageEnvelope env) {
        if (env.mongoId() == null) {
            return Optional.of(new ResolvedMessage(
                env.messageId(),
                DeliveryPath.INLINE,
                env.payload(),
                env.payloadSizeBytes()
            ));
        }
        return reader.fetch(env.mongoId()).map(payload -> new ResolvedMessage(
            env.messageId(),
            DeliveryPath.CLAIM_CHECK,
            payload,
            env.payloadSizeBytes()
        ));
    }
}
