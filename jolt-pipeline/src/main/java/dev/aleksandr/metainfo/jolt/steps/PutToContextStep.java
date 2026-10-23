package dev.aleksandr.metainfo.jolt.steps;

import com.bazaarvoice.jolt.JsonUtils;
import reactor.core.publisher.Flux;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;

import java.util.Map;

/**
 * Записывает значение в контекст, на вход должна приходить Map<String, Object>
 */
@Step
public class PutToContextStep extends OperationStep {
    public static final String PUT_TO_CONTEXT = "putToContext";

    public PutToContextStep(JoltPipeHandler process) {
        super(process);
    }

    @Override
    public String getStepName() {
        return PUT_TO_CONTEXT;
    }

    @Override
    public Flux<Object> apply(Flux<Object> fluxObject, Map<String, Object> specMap, SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars) {
        return fluxObject.doOnNext(input -> {
            if (globalVars != null) {
                Map<String, Object> inputMap = (Map<String, Object>) JsonUtils.cloneJson(input);
                globalVars.putAll(inputMap);
            }
        });
    }

}