package io.github.alex59.metainfo.jolt.transform;

import io.github.alex59.metainfo.jolt.utils.SpecUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Фильтрует данные по полю в объектах массива может быть заменено на jolt
 * {
 *     "operation": "shift",
 *     "spec": {
 *       "*": {
 *         "valueBool": {
 *           "true": {
 *             "@2": "[]"
 *           }
 *         }
 *       }
 *     }
 *   }
 */
public class FilterArrayTransform extends ContextualTransformExt {

    private static final String OPERATIONS = "operations";
    private static final String TYPE = "type";
    private static final String FROM = "from";
    private static final String VALUE = "value";
    private static final String VALUES = "values";
    public static final String DOT_KEY = ".";
    public static final String ESC_DOT = "\\.";

    @Override
    public Object transform(Object input, OperationContext ctx) {
        Map<String, Object> operations = ctx.get(OPERATIONS);

        OperationType operation = OperationType.valueOf(String.valueOf(operations.get(TYPE)).toUpperCase());
        String from = (String)operations.get(FROM);
        String value = (String)operations.get(VALUE);
        List<String> values = (List<String>) operations.get(VALUES);
        if (values == null) {
            values = new ArrayList<>();
            values.add(value);
        }

        List<Object> result = (List<Object>) SpecUtils.copyObject(input);

        List<String> finalValues = values;
        return result.stream().filter(element -> {
            Object finaleElement = findElement(from, element);
            return finalValues.stream().map(val -> operation.check(finaleElement, val))
                .reduce(operation::reduce).orElse(false);
        }).toList();
    }

    enum OperationType {
        EQUALS {
            @Override
            public boolean check(Object value, Object compareValue) {
                return Objects.equals(value, compareValue);
            }

            @Override
            public boolean reduce(Boolean value, Boolean compareValue) {
                return value || compareValue;
            }
        },

        NOT_EQUALS {
            @Override
            public boolean check(Object value, Object compareValue) {
                return !Objects.equals(value, compareValue);
            }

            @Override
            public boolean reduce(Boolean value, Boolean compareValue) {
                return value && compareValue;
            }
        };

        abstract boolean check(Object input, Object condition);

        abstract boolean reduce(Boolean value, Boolean compareValue);
    }


    private Object findElement(String from, Object element) {
        if (from.contains(DOT_KEY)) {
            String[] paths = from.split(ESC_DOT);
            Object cValue = element;
            for (String path : paths) {
                cValue = ((Map<String, Object>)cValue).get(path);
            }
            return cValue;
        } else {
            return ((Map<String, Object>)element).get(from);
        }
    }
}
