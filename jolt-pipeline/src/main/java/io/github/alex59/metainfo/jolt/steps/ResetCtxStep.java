package io.github.alex59.metainfo.jolt.steps;

import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import io.github.alex59.metainfo.jolt.JoltPipeHandler;
import io.github.alex59.metainfo.jolt.model.JoltLoggingProperties;

import java.util.Map;

/**
 * Помещает бизнес контекст в выходной поток
 */
@Step
public class ResetCtxStep extends OperationStep {
    public static final String RESET_CONTEXT = "resetContext";

    public ResetCtxStep(JoltPipeHandler process) {
        super(process);
    }

    @Override
    public String getStepName() {
        return RESET_CONTEXT;
    }

    @Override
    protected Flux<Object> prepareTransform(Flux<Object> fluxObject, Map<String, Object> globalVars, Map<String, Object> specMap,
                                            Map<String, Object> transformContextLocal,
                                            Logger reactContextLogger,
                                            JoltLoggingProperties logging, String comment, Object businessContext) {
        return fluxObject.map(x -> businessContext);
    }

}