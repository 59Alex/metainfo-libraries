package dev.aleksandr.metainfo.jolt.steps;

import com.bazaarvoice.jolt.Chainr;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;
import dev.aleksandr.metainfo.jolt.model.JoltLoggingProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static dev.aleksandr.metainfo.jolt.transform.OperationContext.GLOBAL_VARIABLES;
import static dev.aleksandr.metainfo.jolt.transform.OperationContext.TECH_CONTEXT;

/**
 * Шаг операции jolt см  <a href="https://github.com/bazaarvoice/jolt?tab=readme-ov-file#-documentation">...</a>
 */
@Step
public class OperationStep extends BaseStep {

    public static final String NOT_EMPTY_FILTER = "Фильтр не пустых";
    public static final String IS_STOPPED_FUNCTION = "isStopped";
    public static final String ENRICH_FROM_CONTEXT = "enrichFromContext";
    public static final String TR_IDX4LOG = "transformIndex";

    public static final String BUSINESS_CONTEXT = "_businessContext";
    public static final String OPERATION = "operation";
    public static final String LOG_PROPS = "LOG-PROPS";
    public static final String FLOW_ID = "flowId";

    public OperationStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return OPERATION;
    }

    @Override
    public Flux<Object> apply(Flux<Object> fluxObject, Map<String, Object> specMap, SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars) {
        // читаем из конфига
        Map<String, Object> transformContext = getTypeFromMap(specMap, JOLT_SPEC_KEY);
        Map<String, Object> transformContextLocal = new HashMap<>(
                Optional.ofNullable(transformContext).orElse(new HashMap<>()));
        if (globalVars != null) {
            transformContextLocal.put(GLOBAL_VARIABLES, globalVars);
        }
        transformContextLocal.put(BUSINESS_CONTEXT, specContext.businessContext());
        transformContextLocal.put(TECH_CONTEXT, specContext.techContext());
        transformContextLocal.put(LOG, specContext.log());
        transformContextLocal.put(LOG_PROPS, specContext.logging());
        Object parentSpecIdx = specMap.get(PARENT_SPEC_IDX4LOG);
        transformContextLocal.put(PARENT_SPEC_IDX4LOG, parentSpecIdx);
        transformContextLocal.put(IDX4LOG, specIdxLocal);
        transformContextLocal.put(LEVEL4LOG, level);
        transformContextLocal.put(IS_STOPPED_FUNCTION, specContext.pipeSettings().isPipeStopped());
        transformContextLocal.put(FLOW_ID, specContext.pipeSettings().inputGlobalVars().get(FLOW_ID));
        String comment = getComment(specMap);
        fluxObject = logStep(fluxObject, specMap, specContext.logging(), specContext.log());
        fluxObject = prepareTransform(fluxObject, globalVars, specMap, transformContextLocal, specContext.log(), specContext.logging(), comment, specContext.businessContext());
        return fluxObject;
    }

    public Flux<Object> logStep(Flux<Object> fluxObject, Map<String, Object> specMap, JoltLoggingProperties logging, Logger reactContextLogger) {
        fluxObject = fluxObject.contextWrite(ctx -> ctx.put(TR_IDX4LOG, ctx.getOrDefault(TR_IDX4LOG, 0) + 1));
        String comment = String.valueOf(specMap.get(SPEC_COMMENT));
        fluxObject = fluxObject.map(x -> logTransform(x, specMap, comment, specMap, logging, reactContextLogger));
        return fluxObject;
    }

    protected Flux<Object> prepareTransform(Flux<Object> fluxObject, Map<String, Object> globalVars, Map<String, Object> specMap,
                                            Map<String, Object> transformContextLocal,
                                            Logger reactContextLogger,
                                            JoltLoggingProperties logging, String comment, Object businessContext) {

        fluxObject = fluxObject.flatMap(x -> Mono.fromCallable(() -> {
            Map<String, Object> specMapLocal = specMap;
            if (specMapLocal.containsKey(ENRICH_FROM_CONTEXT)) {
                specMapLocal = new HashMap<>(specMap);
                specMapLocal.computeIfPresent(JOLT_SPEC_KEY,
                        (key, joltSpec) -> enrichFromContext((Map<String, Object>) joltSpec, transformContextLocal));
            }
            Object transformResult = transform(transformContextLocal, x, specMapLocal);
            return handleTransformResult(comment, specMapLocal, transformResult, logging, reactContextLogger);
        }).onErrorResume(
                handleTransformError(specMap, transformContextLocal, comment, logging, x, reactContextLogger)
        ));
        fluxObject = fluxObject.filter(x -> x != Mono.empty()).name(NOT_EMPTY_FILTER);
        return fluxObject;
    }

    protected Object transform(Map<String, Object> transformContextLocal, Object x, Map<String, Object> specMapLocal) {
        if (!(x instanceof List || x instanceof Map) && !specMapLocal.containsKey(OPERATION)) {
            throw new RuntimeException("Jolt transform support only List or Map as input");
        }
        Chainr chainr = Chainr.fromSpec(List.of(specMapLocal));
        return chainr.transform(x, transformContextLocal);
    }

    protected Object handleTransformResult(String comment, Map<String, Object> specMapLocal, Object transformResult,
                                           JoltLoggingProperties logging, Logger reactContextLogger) {
        if (transformResult == null) {
            if (specMapLocal.containsKey(OR_ELSE_KEY)) {
                Object anElse = specMapLocal.get(OR_ELSE_KEY);
                transformResult = ifEmpty(anElse);
            } else {
                if (logging.isInfoEnabled()) {
                    reactContextLogger.info("Transform '{}' returned null(maybe incorrect)", comment);
                }
            }
        }
        return transformResult;
    }

    protected Function<Throwable, Mono<Object>> handleTransformError(Map<String, Object> specMap,
                                                                Map<String, Object> transformContextLocal, String comment, JoltLoggingProperties logging, Object x,
                                                                Logger reactContextLogger) {
        return e -> {
            String errorDesc = logOnError(x, specMap, comment, transformContextLocal, logging, e, reactContextLogger);
            return Mono.error(new Throwable(errorDesc, e));
        };
    }

}