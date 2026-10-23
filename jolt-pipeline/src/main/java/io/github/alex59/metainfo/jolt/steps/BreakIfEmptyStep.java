package io.github.alex59.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import io.github.alex59.metainfo.jolt.JoltPipeHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;


/**
 * Проверяет наличие элементов в потоке, и прерывает выполнение если пусто
 */
@Step
public class BreakIfEmptyStep extends BaseCompositeStep {

    public static final String TYPE = "type";
    public static final String BREAK_IF_EMPTY_KEY = "breakIfEmpty";
    public static final String LIST_KEY = "List";
    public static final String ASTER_KEY = "*";
    public static final String MAP_KEY = "Map";


    public static final String FILTER = "filter";

    public BreakIfEmptyStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return BREAK_IF_EMPTY_KEY;
    }

    // TODO Упростить до одной строки и добавить обработка пути jolt
    /**
     * Проверяет наличие элементов в потоке, и прерывает выполнение если пусто
     * Умеет обрабатывать внутри объекта
     * @param fluxObject - поток объектов(может содержать коллекцию объектов)
     * @param specContext - контекст преобразования
     * @param specIdxLocal - индекс шага
     * @param level - уровень отступа шага
     * @param globalVars - глобальные переменные
     * @param specMap - конфигурация преобразования
     * @return - поток объектов
     */
    @Override
    protected Flux<Object> mapFunction(Flux<Object> fluxObject, SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        Map<String, Object> transformContext = getTypeFromMap(specMap, JOLT_SPEC_KEY);
        String filter = (String) transformContext.get(FILTER);
        String type = String.valueOf(transformContext.get(TYPE));
        fluxObject = fluxObject.concatMap(x -> {
            Object value = x;
            if (LIST_KEY.equals(type)) {
                return handleList(x, filter, value);
            } else if (MAP_KEY.equals(type)) {
                return handleMap(x, (Map) value, filter);
            }
            return Flux.just(x);
        }, 1);
        return fluxObject;
    }

    private static Flux<Object> handleMap(Object x, Map<String,Object> value, String filter) {
        if (value.get(filter) == null) {
            return Flux.empty();
        }
        return Flux.just(x);
    }

    private static Flux<Object> handleList(Object x, String filter, Object value) {
        if (!ASTER_KEY.equals(filter)) {
            if (filter.contains(".")) {
                value = processDot(filter, value);
            } else {
                value = ((Map<String, Object>) x).get(filter);
            }
        }
        if (CollectionUtils.isEmpty((List) value)) {
            return Flux.empty();
        }
        return Flux.just(x);
    }

    private static Object processDot(String filter, Object value) {
        String[] paths = filter.split("\\.");
        Object cValue = value;
        for (String path : paths) {
            if (cValue instanceof Map) {
                cValue = ((Map<String, Object>) cValue).get(path);
            } else if (cValue instanceof List) {
                // работает только для первого элемента в массиве
                cValue = ((Map<String, Object>) ((List) cValue).get(0)).get(path);
            }
        }
        value = cValue;
        return value;
    }

    @Override
    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> zipSpecs) {
        return null;
    }

    @Override
    protected String[] ignoreInLogs(Map<String, Object> specMap) {
        return new String[0];
    }
}