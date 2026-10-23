package io.github.alex59.metainfo.jolt.transform.http;


import com.bazaarvoice.jolt.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import io.github.alex59.metainfo.jolt.model.JoltLoggingProperties;
import io.github.alex59.metainfo.jolt.ports.GenericHttpPort;
import io.github.alex59.metainfo.jolt.steps.OperationStep;
import io.github.alex59.metainfo.jolt.transform.NoBlockContextualTransformExt;
import io.github.alex59.metainfo.jolt.transform.OperationContext;

import java.time.Duration;
import java.time.Instant;
import java.util.SplittableRandom;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static io.github.alex59.metainfo.jolt.steps.BaseStep.IDX4LOG;
import static io.github.alex59.metainfo.jolt.steps.BaseStep.PARENT_SPEC_IDX4LOG;
import static io.github.alex59.metainfo.jolt.steps.OperationStep.IS_STOPPED_FUNCTION;


/**
 * Конфигурируемая трансформация для вызова внешних web сервисов
 */
@Slf4j
public abstract class BaseWebClientTransform<T> extends NoBlockContextualTransformExt<T> {

    public static final String BASE_URL = "baseUrl";
    public static final String PATH = "path";
    public static final String MAX_ATTEMPTS = "maxAttempts";
    public static final String DELAY = "delay";
    public static final String METHOD_KEY = "method";
    public static final String DEFAULT_METHOD = "POST";
    public static final String RQ_MESSAGE = "RQ {} {}_{}: \n{} \n{}";
    public static final String RS_MESSAGE = "RS {} duration: ({}ms) {}_{}: \n{} \n{}";

    private final int retryMaxAttempts;
    private final int retryDelaySec;
    protected final GenericHttpPort genericHttpPort;

    protected BaseWebClientTransform(GenericHttpPort genericHttpPort, int retryMaxAttempts, int retryDelaySec) {
        this.retryMaxAttempts = retryMaxAttempts;
        this.retryDelaySec = retryDelaySec;
        this.genericHttpPort = genericHttpPort;
    }

    @Override
    public abstract Flux<Object> transform(T input, OperationContext ctx);

    public Flux<Object> callHttp(Object input, OperationContext ctx, String uri,
                                 Function<Object, Publisher<String>> httpCallFunction) {

        RetryBackoffSpec backoff = defaultBackoff(ctx);

        int parentSpecIdx = ctx.get(PARENT_SPEC_IDX4LOG);
        int idx = ctx.get(IDX4LOG);

        return Flux.just(input).flatMap(in -> {
            Object randomId = new SplittableRandom().nextInt();
            Instant start = Instant.now();
            return Flux.just(in).doOnNext(
                            inp -> logOnRequest(ctx, RQ_MESSAGE, randomId, parentSpecIdx, idx, uri,
                                    JsonUtils.toPrettyJsonString(inp)))
                    .flatMap(httpCallFunction)
                    .retryWhen(backoff)
                    .doOnNext(out -> {
                        long durationMs = Duration.between(start, Instant.now()).toMillis();
                        logOnRequest(ctx, RS_MESSAGE, randomId, durationMs, parentSpecIdx, idx, uri, out);
                    })
                    .map(JsonUtils::jsonToObject);
        });
    }

    protected String getMethod(OperationContext ctx) {
        return ctx.getOrDefault(METHOD_KEY, DEFAULT_METHOD);
    }

    protected String getUri(OperationContext ctx) {
        String path = ctx.get(PATH);
        return String.join(EMPTY, ctx.get(BASE_URL), path);
    }

    private RetryBackoffSpec defaultBackoff(OperationContext ctx) {
        Consumer<? super Object> isStopped = ctx.get(IS_STOPPED_FUNCTION);
        return Retry.backoff(
                        ctx.getOrDefault(MAX_ATTEMPTS, retryMaxAttempts),
                        Duration.ofSeconds(ctx.getOrDefault(DELAY, retryDelaySec)))
                .jitter(0.5)
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) ->
                        retrySignal.failure()
                )
                .doBeforeRetry(retrySignal ->
                        ctx.getReactiveLogger().warn("Retried {} times, due to: {}", retrySignal.totalRetries(),
                                retrySignal.failure().getMessage()))
                .doAfterRetry(isStopped::accept);
    }

    private void logOnRequest(OperationContext ctx, String rsMessage, Object... params) {
        JoltLoggingProperties logging = ctx.get(OperationStep.LOG_PROPS);
        if (logging.isInfoEnabled()) {
            ctx.getReactiveLogger().info(rsMessage, params);
        }
    }

}
