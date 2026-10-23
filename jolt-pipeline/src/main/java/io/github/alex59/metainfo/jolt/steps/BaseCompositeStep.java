package io.github.alex59.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import io.github.alex59.metainfo.jolt.JoltPipeHandler;
import io.github.alex59.metainfo.jolt.model.JoltLoggingProperties;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 *  Базовый класс компоновки шагов, обычно содержит вложенный операции или условные переходы
 */
abstract class BaseCompositeStep extends BaseStep {
    public static final String OPERATIONS = "operations";
    public static final String USE_INPUT = "useInput";


    protected BaseCompositeStep(JoltPipeHandler process) {
        super(process);
    }

    /**
     * Лог + исполнение
     * @param fluxObject - поток объектов(может содержать коллекцию объектов)
     * @param specMap - конфигурация преобразования
     * @param specContext - контекст преобразования
     * @param specIdxLocal - индекс шага
     * @param level - уровень отступа шага
     * @param globalVars - глобальные переменные
     * @return - поток объектов
     */
    public Flux<Object> apply(Flux<Object> fluxObject, Map<String, Object> specMap, SpecContext specContext,
                              int specIdxLocal, int level, Map<String, Object> globalVars) {
        fluxObject = logStep(fluxObject, specMap, specContext.logging(), specContext.log());
        return mapFunction(fluxObject, specContext, specIdxLocal, level, globalVars, specMap);
    }

    /**
     * Тип преобразования, flatMap по умолчанию
     * @param fluxObject - поток объектов(может содержать коллекцию объектов)
     * @param specContext - контекст преобразования
     * @param specIdxLocal - индекс шага
     * @param level - уровень отступа шага
     * @param globalVars - глобальные переменные
     * @param specMap - конфигурация преобразования
     * @return - поток объектов
     */
    protected Flux<Object> mapFunction(Flux<Object> fluxObject, SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        return fluxObject.flatMap(execute(specContext, specIdxLocal, level, globalVars, specMap));
    }

    public abstract Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> zipSpecs);

    public Flux<Object> logStep(Flux<Object> fluxObject,
                                Map<String, Object> specMap, JoltLoggingProperties logging, Logger reactContextLogger) {
        Map<String, Object> transformContext = getTypeFromMap(specMap, JOLT_SPEC_KEY);
        String comment = String.valueOf(specMap.get(SPEC_COMMENT));
        return logAndCollapse(fluxObject, logging, reactContextLogger, specMap, transformContext, comment, ignoreInLogs(specMap))
                .name(comment);
    }

    /**
     * Обработать набор шагов из конфигурации specs
     *
     * @param fluxObject    - поток объектов(может содержать коллекцию объектов)
     * @param specs         - набор шагов
     * @param globalVarsIn  - глобальные переменные (входные, не будут модифицироваться)
     * @param parentSpecIdx - индекс родительского шага
     * @param level         - уровень отступа шага
     * @param specContext   -  логер
     * @return - поток объектов
     */
    public Flux<Object> handleSpecs(Flux<Object> fluxObject,
                                    List<Map<String, Object>> specs,
                                    Map<String, Object> globalVarsIn,
                                    int parentSpecIdx, int level,
                                    SpecContext specContext) {
        return process.handleSpecs(fluxObject, specs, globalVarsIn, parentSpecIdx,
                level + 1, specContext);
    }

    /**
     * Лог преобразования, без учета вложенных
     * @param fluxObject - поток объектов(может содержать коллекцию объектов)
     * @param logging - конфигурация логирования
     * @param specMap - конфигурация преобразования
     * @param transformContext - контекст преобразования
     * @param comment - комментарий
     * @param toCollapse - имена шагов, которые не должны быть логированы
     * @return - поток объектов
     */
    public Flux<Object> logAndCollapse(Flux<Object> fluxObject, JoltLoggingProperties logging,
                                       Logger reactContextLogger,
                                       Map<String, Object> specMap, Map<String, Object> transformContext, String comment, String... toCollapse) {
        Map<String, Object> mapToLog = new LinkedHashMap<>(specMap);
        for (String attr : toCollapse) {
            mapToLog.put(attr, List.of(COLLAPSED_STR));
        }
        fluxObject = logFluxExecution(fluxObject, logging, mapToLog, transformContext, comment, reactContextLogger);
        return fluxObject;
    }

    /**
     * Список имен шагов, которые не должны быть логированы
     * @param specMap - конфигурация преобразования
     * @return - имена шагов, которые не должны быть логированы
     */
    protected String[] ignoreInLogs(Map<String, Object> specMap) {
        return new String[]{getStepName()};
    }

}