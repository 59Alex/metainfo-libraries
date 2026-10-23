package io.github.alex59.metainfo.jolt.utils;

import com.bazaarvoice.jolt.utils.JoltUtils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.text.StringSubstitutor;

import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.fasterxml.jackson.core.json.JsonReadFeature.ALLOW_JAVA_COMMENTS;


/**
 * Утилиты для обработки спецификаций jolt
 */
public class SpecUtils {

    public static final String PLACEHOLDER_PATTERN = "\\$\\{(.*)\\}";
    public static final String AT_PLACEHOLDER_PATTERN = "@\\{.*?}";

    //    private static final ObjectMapper yamlMapper = new ObjectMapper( YAMLFactory.builder()
//            .enable(ALLOW_JAVA_COMMENTS).build());
    private static final ObjectMapper yamlMapper = new ObjectMapper(JsonFactory.builder()
            .enable(ALLOW_JAVA_COMMENTS).build());


    @FunctionalInterface
    public interface PlaceHolderResolver<T, U, Z, R> {

        /**
         * Applies this function to the given arguments.
         *
         * @param t the first function argument
         * @param u the second function argument
         * @return the function result
         */
        R apply(T t, U u, Z z);
    }

    /**
     * Рекурсивно заменяет ${} на объект или строку из контекста
     *
     * @param input      json в форме map
     * @param globalVars - глобальный контекст
     * @return - обновленный конфиг
     */
    @SuppressWarnings("unchecked")
    public static <T> T resolvePlaceHolders(Object input,
                                            Map<String, Object> globalVars) {
        return (T) resolvePlaceHolders(input, globalVars, PLACEHOLDER_PATTERN, SpecUtils::resolvePlaceHolderValue);
    }

    @SuppressWarnings("unchecked")
    public static <T> T resolvePathPlaceHolders(Object spec,
                                                Object input) {
        return (T) resolvePlaceHolders(spec, input, AT_PLACEHOLDER_PATTERN, SpecUtils::resolveAtPlaceHolderValue);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> resolvePlaceHoldersInMap(Map<String, Object> input,
                                                               Object globalVars, String placeholderPattern, PlaceHolderResolver<Object, Object, String, Object> replace) {
        Map<String, Object> output = new HashMap<>();
        for (Entry<String, Object> entry : input.entrySet()) {
            String resultKey = entry.getKey();
            Object objValue = entry.getValue();
            Object resultValue = resolvePlaceHolders(objValue, globalVars, placeholderPattern, replace);
            if (hasPlaceHolder(resultKey, placeholderPattern)) {
                Object resolvedKey = replace.apply(globalVars, resultKey, placeholderPattern);
                if (resolvedKey instanceof Map) {
                    output.putAll((Map<String, Object>) resolvedKey);
                    continue;
                } else if (resolvedKey instanceof String) {
                    resultKey = (String) resolvedKey;
                } else if (resolvedKey instanceof Number) {
                    resultKey = String.valueOf(resolvedKey);
                }
            }
            output.put(resultKey, resultValue);
        }
        return output;
    }

    /**
     * Рекурсивно заменяет ${} на объект или строку из контекста
     *
     * @param objValue           json в форме map, list, строки
     * @param globalVars         - глобальный контекст
     * @param placeholderPattern
     * @return - обновленный конфиг
     */
    public static Object resolvePlaceHolders(Object objValue, Object globalVars, String placeholderPattern, PlaceHolderResolver<Object, Object, String, Object> replace) {
        Object resultValue = null;
        if (objValue instanceof Map) {
            resultValue = resolvePlaceHoldersInMap((Map<String, Object>) objValue, globalVars, placeholderPattern, replace);
        } else if (objValue instanceof Collection) {
            resultValue = resolvePlaceHolderInList(globalVars, (List<Object>) objValue, placeholderPattern, replace);
        } else if (objValue != null) {
            resultValue = replace.apply(globalVars, objValue, placeholderPattern);
        }
        return resultValue;
    }

    public static Object resolvePlaceHolderInList(Object globalVars, List<Object> objValue, String placeholderPattern, PlaceHolderResolver<Object, Object, String, Object> replace) {
        List<Object> resArray = new ArrayList<>();
        for (Object arrayValue : objValue) {
            resArray.add(resolvePlaceHolders(arrayValue, globalVars, PLACEHOLDER_PATTERN, replace));
        }
        return resArray;
    }

    public static Object resolvePlaceHolderValue(Object globalVars, Object value, String placeholderPattern) {
        Object resultValue = value;
        String valueStr = value.toString();
        if (hasPlaceHolder(valueStr, placeholderPattern)) {
            Map<String, Object> globalVarsMap = (Map<String, Object>) globalVars;
            if (hasPlaceHolderOnly(valueStr, placeholderPattern)) {
                String placeholderName = getPlaceholderName(valueStr);
                resultValue = copyObject(globalVarsMap.get(placeholderName));
            } else {
                resultValue = replaceVariables(valueStr, globalVarsMap);
            }
        }
        return resultValue;
    }

    private static Object resolveAtPlaceHolderValue(Object input, Object value, String placeholderPattern) {
        Object resultValue = value;
        String valueStr = value.toString();
        if (hasAtPlaceHolder(valueStr)) {
            if (hasAtPlaceHolderOnly(valueStr)) {
                String joltPath = valueStr.substring(2, valueStr.length() - 1);
                resultValue = copyObject(resolvePath(joltPath, input));
            } else {
                resultValue = resolveAtPlaceholders(valueStr, input);
            }
        }
        return resultValue;
    }

    /**
     * Рекурсивно копирует объект (shallowCopy)
     *
     * @param input - json форме map/list
     * @return копия
     */
    @SuppressWarnings("unchecked")
    public static <T> T copyObject(Object input) {
        if (input instanceof Map) {
            Map<String, Object> map = new HashMap<>();
            for (Entry<String, Object> entry : ((Map<String, Object>) input).entrySet()) {
                map.put(entry.getKey(), copyObject(entry.getValue()));
            }
            return (T) map;
        } else if (input instanceof List<?> list) {
            List<Object> objects = new ArrayList<>();
            for (Object obj : list) {
                objects.add(copyObject(obj));
            }
            return (T) objects;
        } else {
            return (T) input;
        }

    }

    private static boolean hasPlaceHolder(String key, String placeholderPattern) {
        return Pattern.compile(placeholderPattern).matcher(key).find();
    }

    private static boolean hasPlaceHolderOnly(String key, String placeholderPattern) {
        return Pattern.compile(placeholderPattern).matcher(key).matches();
    }

    private static boolean hasAtPlaceHolder(String key) {
        return Pattern.compile(AT_PLACEHOLDER_PATTERN).matcher(key).find();
    }

    private static boolean hasAtPlaceHolderOnly(String key) {
        return Pattern.compile(AT_PLACEHOLDER_PATTERN).matcher(key).matches();
    }

    private static String getPlaceholderName(String key) {
        Matcher matcher = Pattern.compile(PLACEHOLDER_PATTERN).matcher(key);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    public static String replaceVariables(String jsonConfigString, Map<String, Object> jolt) {
        StringSubstitutor globalVarsSubstitutor = new StringSubstitutor(jolt);
        jsonConfigString = globalVarsSubstitutor.replace(jsonConfigString);
        return jsonConfigString;
    }

    /**
     * @param message    сообщения с плейсхолдерами  @{conditions.0}
     * @param jsonObject - входные данные Map/List в jolt формате
     * @return сообщение с найденными подстановками
     */
    public static String resolveAtPlaceholders(String message, Object jsonObject) {
        Pattern pattern = Pattern.compile(AT_PLACEHOLDER_PATTERN);
        Matcher matcher = pattern.matcher(message);
        String result = message;
        while (matcher.find()) {
            String fullPlaceholder = matcher.group();
            String joltPath = fullPlaceholder.substring(2, fullPlaceholder.length() - 1);
            Object resolved = resolvePath(joltPath, jsonObject);
            result = result.replace(fullPlaceholder, resolved.toString());
        }
        return result;
    }

    public static <T> T parseConfig(String jsonConfigString) throws JsonProcessingException {
        return yamlMapper.readerFor(new TypeReference<T>() {
        }).readValue(jsonConfigString);
    }

    /**
     * @param joltPath путь jolt в одной строке, например conditions.0.conditionValues.0.value
     * @param inputSp  входные данные Map/List
     * @return результат прохождения по пути
     */
    public static Object resolvePath(String joltPath, Object inputSp) {
        String[] split = joltPath.split("\\.");
        Object[] path = new Object[split.length];
        for (int i = 0; i < split.length; i++) {
            String pathEntry = split[i];
            if (pathEntry.matches("\\d")) {
                path[i] = Integer.valueOf(pathEntry);
            } else {
                path[i] = pathEntry;
            }
        }
        return JoltUtils.navigate(inputSp, path);
    }

}
