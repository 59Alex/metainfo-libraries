package io.github.alex59.metainfo.jolt.transform;

import com.bazaarvoice.jolt.ContextualTransform;

import java.util.Map;

/**
 * Обертка для конвертирования контекста преобразований
 */
public abstract class GenericContextualTransformExt<T> implements ContextualTransform {



    @Override
    @SuppressWarnings("unchecked")
    public Object transform(Object input, Map<String, Object> context) {
        return transform((T) input, new OperationContext(context));
    }


    public abstract Object transform(T input, OperationContext ctx);

}
