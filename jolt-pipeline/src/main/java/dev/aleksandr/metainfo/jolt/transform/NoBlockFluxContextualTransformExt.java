package dev.aleksandr.metainfo.jolt.transform;

import com.bazaarvoice.jolt.ContextualTransform;
import reactor.core.publisher.Flux;

import java.util.Map;

/**
 * Обертка для поддержки неблокирующих вызовов от spring components
 */
public abstract class NoBlockFluxContextualTransformExt<T> implements ContextualTransform {

    @Override
    public Flux<Object> transform(Object input, Map<String, Object> context) {
        return transform((T) input, new OperationContext(context));
    }

    public abstract Flux<Object> transform(T input, OperationContext ctx);
}
