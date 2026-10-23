package io.github.alex59.metainfo.jolt.utils;

import lombok.experimental.UtilityClass;

import java.util.Map;

/**
 * Класс обертка, что бы не добавлять прямой зависимость от jolt json utils в клиентскую библиотеку
 */
@UtilityClass
public class JsonUtils {

    public static String toJsonString(Object obj) {
       return com.bazaarvoice.jolt.JsonUtils.toJsonString(obj);
    }

    public static Object filepathToObject(String path) {
        return com.bazaarvoice.jolt.JsonUtils.filepathToObject(path);
    }

    public static String toPrettyJsonString(Object input) {
        return com.bazaarvoice.jolt.JsonUtils.toPrettyJsonString(input);
    }

    public static Object jsonToObject(String json) {
        return com.bazaarvoice.jolt.JsonUtils.jsonToObject(json);
    }

    public static Object cloneJson(Object o) {
        return com.bazaarvoice.jolt.JsonUtils.cloneJson(o);
    }

    public static Map<String, Object> jsonToMap(String string) {
        return com.bazaarvoice.jolt.JsonUtils.jsonToMap(string);
    }
}
