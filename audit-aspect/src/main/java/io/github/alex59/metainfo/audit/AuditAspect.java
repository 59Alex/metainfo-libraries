package io.github.alex59.metainfo.audit;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

@Aspect
public final class AuditAspect {
    private final AuditSink sink;
    private final Clock clock;

    public AuditAspect(AuditSink sink) {
        this(sink, Clock.systemUTC());
    }

    public AuditAspect(AuditSink sink, Clock clock) {
        this.sink = Objects.requireNonNull(sink);
        this.clock = Objects.requireNonNull(clock);
    }

    @Around("@annotation(audited)")
    public Object audit(ProceedingJoinPoint call, Audited audited) throws Throwable {
        long start = System.nanoTime();
        Object result;
        try {
            result = call.proceed();
        } catch (Throwable error) {
            emit(audited.value(), error, start);
            throw error;
        }
        if (result instanceof Mono<?> mono) {
            return Mono.defer(() -> {
                long subscribed = System.nanoTime();
                AtomicBoolean emitted = new AtomicBoolean();
                return mono
                        .doOnSuccess(value -> emitOnce(audited.value(), null, subscribed, emitted))
                        .doOnError(error -> emitOnce(audited.value(), error, subscribed, emitted));
            });
        }
        if (result instanceof Flux<?> flux) {
            return Flux.defer(() -> {
                long subscribed = System.nanoTime();
                AtomicBoolean emitted = new AtomicBoolean();
                return flux
                        .doOnComplete(() -> emitOnce(audited.value(), null, subscribed, emitted))
                        .doOnError(error -> emitOnce(audited.value(), error, subscribed, emitted));
            });
        }
        emit(audited.value(), null, start);
        return result;
    }

    private void emitOnce(String action, Throwable error, long start, AtomicBoolean emitted) {
        if (emitted.compareAndSet(false, true)) {
            emit(action, error, start);
        }
    }

    private void emit(String action, Throwable error, long start) {
        sink.write(new AuditRecord(action, Instant.now(clock), error == null,
                error == null ? null : error.getClass().getSimpleName(),
                System.nanoTime() - start));
    }
}
