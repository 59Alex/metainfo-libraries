package dev.aleksandr.metainfo.jolt.transform;

import dev.aleksandr.metainfo.jolt.exceptions.ObsoleteResourceException;

import java.util.List;

public class ThrowIfEmptyTransform extends GenericContextualTransformExt<List<Object>> {


    @Override
    public List<Object> transform(List<Object> input, OperationContext ctx) {

        if (input != null && input.isEmpty()) {
            String message = ctx.get("message");
            throw new ObsoleteResourceException(message);
        }
        return input;
    }
}
