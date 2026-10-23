package dev.aleksandr.metainfo.jolt.transform;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import dev.aleksandr.metainfo.jolt.ports.GenericHttpPort;
import dev.aleksandr.metainfo.jolt.transform.http.BaseWebClientTransform;


/**
 * Конфигурируемая трансформация для вызова внешних web сервисов
 */
@Slf4j
@Component
public class CallWebClient extends BaseWebClientTransform<Object> {

    public CallWebClient(@SuppressWarnings("Autowired") GenericHttpPort genericHttpPort,
                         @Value("${metainfo.jolt.http.retry-max-attempts:7}")  int retryMaxAttempts,
                         @Value("${metainfo.jolt.http.retry-delay-sec:2}") int retryDelaySec) {
        super(genericHttpPort, retryMaxAttempts, retryDelaySec);
    }

    @Override
    public Flux<Object> transform(Object input, OperationContext ctx) {
        String httpMethod = getMethod(ctx);
        String uri = getUri(ctx);
        return callHttp(input, ctx, uri, in -> genericHttpPort.callHttp(input,
            httpMethod, uri));
    }
}
