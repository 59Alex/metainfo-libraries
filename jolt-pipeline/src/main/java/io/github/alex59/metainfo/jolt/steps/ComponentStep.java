package io.github.alex59.metainfo.jolt.steps;

import reactor.core.publisher.Flux;
import io.github.alex59.metainfo.jolt.JoltPipeHandler;

import java.util.Map;

/**
 * @deprecated use {@link PureComponentStep}
 */
@Deprecated
@Step
public class ComponentStep extends PureComponentStep {


    public static final String COMPONENT_KEY = "oldComponent";
    public static final String PARAMS_KEY = "params";


    public ComponentStep(JoltPipeHandler process) {
        super(process);
    }

    @Override
    public String getStepName() {
        return COMPONENT_KEY;
    }

    @Override
    public Flux<Object> apply(Flux<Object> fluxObject, Map<String, Object> specMap, SpecContext specContext,
                              int specIdxLocal, int level, Map<String, Object> globalVars) {

        Map<String, Object> variables = globalVars;
        Map<String, Object> params = getTypeFromMap(specMap, PARAMS_KEY);
        if (params != null) {
            if (variables != null) {
                variables.putAll(params);
            } else {
                variables = params;
            }
        }
        String comment = String.valueOf(specMap.get(SPEC_COMMENT));
        Map<String, Object> transformContext = getTypeFromMap(specMap, JOLT_SPEC_KEY);
        fluxObject = logFluxExecution(fluxObject, specContext.logging(), specMap,
                transformContext, comment, specContext.log());

        return applyComponent(String.valueOf(specMap.get(COMPONENT_KEY)),
                fluxObject, variables, specIdxLocal, level + 1, specContext);
    }

}