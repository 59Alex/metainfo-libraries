package io.github.alex59.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import io.github.alex59.metainfo.jolt.JoltPipeHandler;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Выполнение шага при ошибке повторяется до тех пор, пока не будет выполнено определенное количество попыток(maxAttempts)
 * или не будет выполнено определенное время ожидания(exp(delaySec))
 */
@Step
public class RetryStep extends BaseCompositeStep {
    public static final String MAX_ATTEMPTS = "maxAttempts";
    public static final String DELAY = "delaySec";
    public static final String RETRY_ON_ERROR = "retryOnError";

    public RetryStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return RETRY_ON_ERROR;
    }

    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal,
                                                  int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        int maxAttempts = getTypeFromMap(specMap, MAX_ATTEMPTS);
        int delaySec = getTypeFromMap(specMap, DELAY);
        List<Map<String, Object>> childSpecs = getTypeFromMap(specMap, getStepName());
        return x -> handleSpecs(Flux.just(x), childSpecs, globalVars,
                specIdxLocal, level + 1, specContext)
                .retryWhen(backoffSpec(specContext.log(),
                        maxAttempts, delaySec, specContext.pipeSettings().isPipeStopped()));
    }

    private RetryBackoffSpec backoffSpec(Logger log, int maxAttempts, int delaySec,
                                         Consumer<? super Object> isExecutionStopped) {
        return Retry.backoff(
                        maxAttempts,
                        Duration.ofSeconds(delaySec))
                .jitter(0.5)
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) ->
                        retrySignal.failure()
                )
                .doBeforeRetry(retrySignal ->
                        log.warn("Specs retried {} times, due to: {}", retrySignal.totalRetries(),
                                retrySignal.failure().getMessage()))
                .doAfterRetry(isExecutionStopped::accept);
    }

}