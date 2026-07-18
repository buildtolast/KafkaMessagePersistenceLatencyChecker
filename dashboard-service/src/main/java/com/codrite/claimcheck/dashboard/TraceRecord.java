package com.codrite.claimcheck.dashboard;

import java.util.List;

public record TraceRecord(String pairId, String path, List<HopRecord> hopTrace) {}
