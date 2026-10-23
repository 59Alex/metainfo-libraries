package io.github.alex59.metainfo.audit;

import java.time.Instant;

public record AuditRecord(
        String action,
        Instant completedAt,
        boolean success,
        String errorType,
        long durationNanos
) {
}
