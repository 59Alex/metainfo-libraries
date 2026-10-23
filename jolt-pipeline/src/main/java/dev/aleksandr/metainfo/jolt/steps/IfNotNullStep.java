package dev.aleksandr.metainfo.jolt.steps;

import dev.aleksandr.metainfo.jolt.JoltPipeHandler;

import java.util.Map;

/**
 * Если значение не null, то выполняет шаг
 */
@Step
public class IfNotNullStep extends IfStep {

    public static final String IF_NOT_EMPTY_KEY = "ifNotNull";

    public IfNotNullStep(JoltPipeHandler process) {
        super(process);
    }

    @Override
    public String getStepName() {
        return IF_NOT_EMPTY_KEY;
    }

    @Override
    protected String getVarInput(Map<String, Object> specMap) {
        return getTypeFromMap(specMap, IF_NOT_EMPTY_KEY);
    }
}