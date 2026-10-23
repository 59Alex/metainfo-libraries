package io.github.alex59.metainfo.jolt.transform;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class SetDateNowDateToListTransform extends ContextualTransformExt {

    public static final String FIELD_NAME_KEY = "fieldName";

    public static String nowDateString() {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(OffsetDateTime.now());
    }


    @Override
    @SuppressWarnings("unchecked")
    public Object transform(Object input, OperationContext ctx) {
        List<Map<String, Object>> inputList = (List<Map<String, Object>>) input;
        String fieldName = ctx.get(FIELD_NAME_KEY);
        String format = nowDateString();
        for (Map<String, Object> item : inputList) {
            item.put(fieldName, format);
        }
        return input;
    }
}
