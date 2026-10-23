package dev.aleksandr.metainfo.jolt.steps;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;
import dev.aleksandr.metainfo.jolt.model.JoltLoggingProperties;
import dev.aleksandr.metainfo.jolt.model.PipeSettings;
import dev.aleksandr.metainfo.jolt.utils.SpecUtils;

import java.io.IOException;
import java.util.*;

import static dev.aleksandr.metainfo.jolt.transform.OperationContext.GLOBAL_VARIABLES;

/**
 * Базовая реализация шага преобразования
 */
@Getter
@AllArgsConstructor
@Step
public abstract class BaseStep {
    public static final String SIMPLE_MODULE = "SimpleModule";
    public static final String JOLT_SPEC_KEY = "spec";
    public static final String SPEC_COMMENT = "_comment";
    public static final String OR_ELSE_KEY = "orElse";
    public static final String EMPTY_LIST_KEY = "emptyList";
    public static final String BREAK_KEY = "break";
    public static final String LEVEL4LOG = "level";
    public static final String IDX4LOG = "idx";
    public static final String PARENT_SPEC_IDX4LOG = "parentSpecIdx";


    public static final String CONTEXT4LOG = "\n---------------------CONTEXT---------------------------\n";
    public static final String LOG_SEP = "\n=======================================================\n";
    public static final String CONFIG4LOG = "-----------------------CONFIG--------------------------\n";
    public static final String INPUT4OG = "\n---------------------INPUT---------------------------\n";
    public static final String TAB_KEY = "  ";
    public static final String WHITESPACE4LOG = " ";
    public static final String NULL4LOG = "null";
    public static final String COLLAPSED_STR = "...";
    public static final String LOG = "LOG";
    public static final String CR_LOG = "\n";
    public static final String SUB4LOG = "_";


    public static final ObjectMapper defaultMapper = new ObjectMapper();
    public static final ObjectMapper excludedIndexesMapper = excludedIndexesMapper();
    private static final String LOG_KEY = "logMap";

    protected JoltPipeHandler process;

    public record SpecContext(
            Map<String, Object> specMap,
            PipeSettings pipeSettings) {

        public Object businessContext() {
            return pipeSettings.businessContext();
        }

        public Object techContext() {
            return pipeSettings.techContext();
        }

        public Logger log() {
            return pipeSettings.reactContextLogger();
        }

        public JoltLoggingProperties logging() {
            return pipeSettings.logProps();
        }
    }

    /**
     * Имя шага, ключевой атрибут шага json
     *
     * @return имя шага используемое, как ключ определения преобразования
     */
    public abstract String getStepName();

    /**
     * Выполняется перед применением шага
     *
     * @param specMap       - json контекст преобразования
     * @param specIdx       - индекс шага
     * @param parentSpecIdx - индекс родительского шага
     * @param level         - уровень отступа шага
     */
    public void beforeApply(Map<String, Object> specMap, int specIdx, int parentSpecIdx, int level) {
        specMap.put(PARENT_SPEC_IDX4LOG, parentSpecIdx);
        specMap.put(IDX4LOG, specIdx);
        specMap.put(LEVEL4LOG, level);
    }


    /*
     * Вызывает препроцессор и вызывает метод преобразования
     * @param fluxObject - поток объектов
     * @param specMap - json контекст преобразования
     * @param specContext - контекст преобразования
     * @param specIdxLocal - индекс шага
     * @param level - уровень отступа шага
     * @param parentSpecIdx - индекс родительского шага
     * @param globalVars - глобальные переменные
     * @return поток объектов
     */
    public final Flux<Object> applyStep(Flux<Object> fluxObject, Map<String, Object> specMap, SpecContext specContext,
                                        int specIdxLocal, int level, int parentSpecIdx, Map<String, Object> globalVars) {
        beforeApply(specMap, specIdxLocal, parentSpecIdx, level);
        return apply(fluxObject, specMap, specContext, specIdxLocal, level, globalVars);
    }

    /**
     * Вызывает метод преобразования
     *
     * @param fluxObject   - поток объектов(может содержать коллекцию объектов)
     * @param specMap      - конфигурация преобразования
     * @param specContext  - контекст преобразования
     * @param specIdxLocal - индекс шага
     * @param level        - уровень отступа шага
     * @param globalVars   - глобальные переменные
     * @return поток объектов
     */
    public abstract Flux<Object> apply(Flux<Object> fluxObject, Map<String, Object> specMap, SpecContext specContext,
                                       int specIdxLocal, int level, Map<String, Object> globalVars);

    /**
     * Получает типизированные значение из контекста преобразования
     * @param specMap - конфигурация преобразования
     * @param key - ключ
     * @param <T> - тип значения
     * @return значение
     */
    @SuppressWarnings("unchecked")
    protected <T> T getTypeFromMap(Map<String, Object> specMap, String key) {
        return (T) specMap.get(key);
    }

    /**
     * Получает типизированные значение из контекста преобразования
     * @param specMap - конфигурация преобразования
     * @param key - ключ
     * @param defaultValue - значение по умолчанию
     * @param <T> - тип значения
     * @return значение
     */
    @SuppressWarnings("unchecked")
    protected <T> T getTypeFromMap(Map<String, Object> specMap, String key, Object defaultValue) {
        Object o = specMap.get(key);
        if (o == null) {
            return (T) defaultValue;
        }
        return (T) o;
    }

    /**
     * Обогащает контекст преобразования данными из глобальных переменных.
     * Пример: Заменяет заполнитель ${var} на значение из globalVars Map
     *
     * @param specMap               - конфигурация преобразования
     * @param transformContextLocal - контекст преобразования
     * @return обогащенный контекст преобразования
     */
    @SuppressWarnings("unchecked")
    protected Map<String, Object> enrichFromContext(Map<String, Object> specMap,
                                                    Map<String, Object> transformContextLocal) {
        Map<String, Object> globalVars = (Map<String, Object>) transformContextLocal.get(GLOBAL_VARIABLES);
        return SpecUtils.resolvePlaceHolders(specMap, globalVars);
    }

    /**
     * Заменяет строку(emptyList, break) конфигурации пустого объекта на
     *
     * @param anElse - конфигурация пустого объекта
     * @return сконфигурированный объект
     */
    protected Object ifEmpty(Object anElse) {
        Object transformResult;
        if (anElse instanceof String anElseString) {
            transformResult = switch (anElseString) {
                case EMPTY_LIST_KEY -> new ArrayList<>();
                case BREAK_KEY -> Mono.empty();
                default -> new HashMap<>();
            };
        } else if (anElse instanceof Map || anElse instanceof List) {
            transformResult = anElse;
        } else {
            transformResult = new HashMap<>();
        }
        return transformResult;
    }

    /**
     * Логирует выполнение преобразования
     *
     * @param fluxObject         - поток объектов
     * @param logging            - конфигурация логирования
     * @param specMap            - конфигурация преобразования
     * @param transformContext   - контекст преобразования
     * @param comment            - комментарий (имя)
     * @param reactContextLogger - логгер React
     * @return поток объектов
     */
    protected Flux<Object> logFluxExecution(Flux<Object> fluxObject, JoltLoggingProperties logging,
                                            Map<String, Object> specMap, Map<String, Object> transformContext, String comment, Logger reactContextLogger) {
        fluxObject = fluxObject.map(x -> logTransform(x, specMap, comment, transformContext, logging, reactContextLogger))
                .name(LOG_KEY);
        return fluxObject;
    }

    /**
     * Логирует выполнение преобразования
     *
     * @param x                     - объект
     * @param specMap               - конфигурация преобразования
     * @param comment               - комментарий (имя)
     * @param transformContextLocal - контекст преобразования
     * @param loggingLevel          - конфигурация логирования
     * @param reactContextLogger    - логгер React
     * @return объект
     */
    @SneakyThrows
    public Object logTransform(Object x, Map<String, Object> specMap, String comment,
                               Map<String, Object> transformContextLocal, JoltLoggingProperties loggingLevel, Logger reactContextLogger) {
        StringBuilder sb = new StringBuilder();

        if (loggingLevel.isInfoEnabled()) {
            printComment(specMap, loggingLevel, sb);
            if (loggingLevel.isTraceEnabled()) {
                if (loggingLevel.isTraceEnabled()) {
                    printConfigs(x, specMap, sb);
                }
                if (loggingLevel.isDetailedEnabled()) {
                    printContext(transformContextLocal, sb);
                }
            }
            if (reactContextLogger.isInfoEnabled()) {
                reactContextLogger.info(sb.toString());
            }
        }
        return x;
    }

    /**
     * Выводит полный контекст преобразования
     *
     * @param transformContextLocal - контекст преобразования
     * @param sb                    - буфер
     * @throws JsonProcessingException - в случае некорректных данных
     */
    private void printContext(Map<String, Object> transformContextLocal, StringBuilder sb) throws JsonProcessingException {
        String ctxJson = defaultMapper.writerWithDefaultPrettyPrinter()
                .writeValueAsString(transformContextLocal);
        sb.append(CONTEXT4LOG)
                .append(ctxJson)
                .append(LOG_SEP);
    }

    /**
     * Формирует строку с конфигурацией преобразования
     *
     * @param x       - объект
     * @param specMap - конфигурация преобразования
     * @param sb      - буфер
     * @throws JsonProcessingException - в случае некорректных данных
     */
    private void printConfigs(Object x, Map<String, Object> specMap, StringBuilder sb) throws JsonProcessingException {
        String specJson = excludedIndexesMapper.writerWithDefaultPrettyPrinter()
                .writeValueAsString(specMap);
        sb.append(CONFIG4LOG);
        sb.append(specJson);
        String inputJson = defaultMapper.writerWithDefaultPrettyPrinter()
                .writeValueAsString(x);
        sb.append(INPUT4OG);
        sb.append(inputJson);
    }

    /**
     * Формирует строку с комментарием(названием)
     *
     * @param specMap      - конфигурация преобразования
     * @param loggingLevel - конфигурация логирования
     * @param sb           - буфер
     * @throws JsonProcessingException - в случае некорректных данных
     */
    private void printComment(Map<String, Object> specMap, JoltLoggingProperties loggingLevel,
                              StringBuilder sb) throws JsonProcessingException {
        if (loggingLevel.isDebugEnabled()) {
            sb.append(LOG_SEP);
        }
        int parentSpecIdx = (int) specMap.get(PARENT_SPEC_IDX4LOG);
        int level = (int) specMap.get(LEVEL4LOG);
        int idx = (int) specMap.get(IDX4LOG);
        String comment = getComment(specMap);
        sb.append(StringUtils.repeat(TAB_KEY, level));
        sb.append(parentSpecIdx);
        sb.append(SUB4LOG);
        sb.append(idx);
        sb.append(WHITESPACE4LOG);
        sb.append(comment);
        if (loggingLevel.isDebugEnabled()) {
            sb.append(LOG_SEP);
        }
    }

    @SneakyThrows
    protected String getComment(Map<String, Object> specMap) {
        String comment = String.valueOf(specMap.get(SPEC_COMMENT));
        if (comment.equals(NULL4LOG)) {
            comment = excludedIndexesMapper.writeValueAsString(specMap);
        }
        return comment;
    }

    /**
     * Формирует строку с ошибкой преобразования с детальным описанием
     *
     * @param x                     - входной объект
     * @param specMap               - конфигурация преобразования
     * @param comment               - комментарий (имя)
     * @param transformContextLocal - контекст преобразования
     * @param loggingLevel          - уровень логирования
     * @param ex                    - исключение
     * @param reactContextLogger    - логгер
     * @return строка с описанием ошибки
     */
    @SneakyThrows
    protected String logOnError(Object x, Map<String, Object> specMap, String comment,
                                Map<String, Object> transformContextLocal, JoltLoggingProperties loggingLevel, Throwable ex,
                                Logger reactContextLogger) {
        StringBuilder sb = new StringBuilder();
        printComment(specMap, loggingLevel, sb);
        printConfigs(x, specMap, sb);
        printContext(transformContextLocal, sb);
        sb.append(CR_LOG);
        sb.append(ex.getMessage());
        if (reactContextLogger.isErrorEnabled()) {
            reactContextLogger.error(sb.toString(), ex);
        }
        return sb.toString();
    }

    /**
     * @return - маппер для исключения технических полей
     */
    @SuppressWarnings("unchecked")
    private static ObjectMapper excludedIndexesMapper() {
        /*Сет полей для исключения*/
        Set<String> excludeSet = Set.of(PARENT_SPEC_IDX4LOG, LEVEL4LOG, IDX4LOG);
        SimpleModule simpleModule = new SimpleModule(SIMPLE_MODULE,
                Version.unknownVersion());

        simpleModule.addSerializer(Map.class, new JsonSerializer<>() {
            @Override
            public void serialize(Map value, JsonGenerator gen, SerializerProvider serializers)
                    throws IOException {
                gen.writeStartObject();
                for (Object entry : value.entrySet()) {
                    Map.Entry<String, Object> pair = (Map.Entry<String, Object>) entry;
                    if (excludeSet.stream().noneMatch(e -> e.equals(pair.getKey()))) {
                        gen.writeFieldName(pair.getKey());
                        gen.writeObject(pair.getValue());
                    }
                }
                gen.writeEndObject();
            }
        });
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(simpleModule);
        return mapper;
    }

    /**
     * Заменяем заполнители @{} и ${} на значения
     * заменитель @{} указывает сокращенный jolt путь внутри входного объекта
     * заменитель %{} указывает имя из глобальных переменных
     *
     * @param specMap    - конфигурация преобразования
     * @param globalVars - глобальные переменные
     * @param x          - входной объект
     * @return - Объект с замененными значениями
     */
    @SuppressWarnings("unchecked")
    public <T> T resolveAllPlaceholders(Object specMap, Map<String, Object> globalVars, Object x) {
        Object enrichedSpec = SpecUtils.resolvePlaceHolders(specMap, globalVars);
        enrichedSpec = SpecUtils.resolvePathPlaceHolders(enrichedSpec,  x);
        return (T) enrichedSpec;
    }
}