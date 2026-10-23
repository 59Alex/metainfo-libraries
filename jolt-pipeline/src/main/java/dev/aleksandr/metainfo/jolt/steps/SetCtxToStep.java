package dev.aleksandr.metainfo.jolt.steps;

import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;
import dev.aleksandr.metainfo.jolt.model.JoltLoggingProperties;
import dev.aleksandr.metainfo.jolt.utils.SpecUtils;

import java.util.Map;


/**
 * Устанавливает контекст в объект находящийся в атрибуте setContextTo
 * при установке enrichFromContext в true поддерживает пути входного контекста @{} и глобальные переменные ${}
 */
@Step
public class SetCtxToStep extends OperationStep {

    public static final String SET_CONTEXT_TO_KEY = "setContextTo";

    public SetCtxToStep(JoltPipeHandler process) {
        super(process);
    }

    @Override
    public String getStepName() {
        return SET_CONTEXT_TO_KEY;
    }

    @Override
    protected Flux<Object> prepareTransform(Flux<Object> fluxObject, Map<String, Object> globalVars, Map<String, Object> specMap,
                                            Map<String, Object> transformContextLocal,
                                            Logger reactContextLogger,
                                            JoltLoggingProperties logging, String comment, Object businessContext) {
        return fluxObject.map(x -> {
            Object specObj;
            if (specMap.containsKey(ENRICH_FROM_CONTEXT)) {
                Map<String,Object> specMapResolved = resolveAllPlaceholders(specMap, globalVars, x);
                specObj = getTypeFromMap(specMapResolved, SET_CONTEXT_TO_KEY);
            } else {
                specObj = getTypeFromMap(specMap, SET_CONTEXT_TO_KEY);
            }
            return SpecUtils.copyObject(specObj);
        });
    }

}