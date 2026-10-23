package io.github.alex59.metainfo.audit;

@FunctionalInterface
public interface AuditSink {
    void write(AuditRecord record);
}
