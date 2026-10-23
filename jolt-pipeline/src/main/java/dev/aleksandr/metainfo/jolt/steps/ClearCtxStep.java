package dev.aleksandr.metainfo.jolt.steps;

import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;
import dev.aleksandr.metainfo.jolt.model.JoltLoggingProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Помещает пустой объект в выходной поток
 */
@Step
public class ClearCtxStep extends OperationStep {
    public static final String CLEAR_CONTEXT = "clearContext";

    public ClearCtxStep(JoltPipeHandler process) {
        super(process);
    }

    @Override
    public String getStepName() {
        return CLEAR_CONTEXT;
    }

    @Override
    protected Flux<Object> prepareTransform(Flux<Object> fluxObject, Map<String, Object> globalVars, Map<String, Object> specMap, Map<String, Object> transformContextLocal, Logger reactContextLogger, JoltLoggingProperties logging, String comment, Object businessContext) {
        return fluxObject.map(x -> new HashMap<>());
    }


}