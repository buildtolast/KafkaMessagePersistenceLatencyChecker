package com.codrite.claimcheck.consumer;

import java.util.List;

public record TraceRecord(String pairId, DeliveryPath path, List<HopRecord> hopTrace) {
}
