package io.github.alex59.metainfo.jolt.transform;


import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class CoalesceTransform extends GenericContextualTransformExt<List<Map<String, Object>>> {

    public Object transform(List<Map<String, Object>> input, OperationContext ctx) {
        List<String> attributes = ctx.get("attrs");
        String outputAttr = ctx.get("output");
        return input.stream().peek(item -> {
            Object coalesceResult = attributes.stream().map(item::get)
                    .filter(attrValue ->
                            attrValue != null &&
                                    StringUtils.isNotBlank(String.valueOf(attrValue)))
                    .findFirst().orElse(null);
            item.put(outputAttr, coalesceResult);
        }).toList();
    }

}
