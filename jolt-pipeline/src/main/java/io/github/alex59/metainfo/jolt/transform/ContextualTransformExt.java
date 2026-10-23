package io.github.alex59.metainfo.jolt.transform;

import com.bazaarvoice.jolt.ContextualTransform;

import java.util.Map;

/**
 * Обертка для конвертирования контекста преобразований
 */
public abstract class ContextualTransformExt implements ContextualTransform {



    @Override
    public Object transform(Object input, Map<String, Object> context) {
        return transform(input, new OperationContext(context));
    }


    public abstract Object transform(Object input, OperationContext ctx);
}
