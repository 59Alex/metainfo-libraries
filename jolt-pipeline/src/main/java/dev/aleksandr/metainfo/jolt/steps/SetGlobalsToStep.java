package dev.aleksandr.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;

import java.util.Map;
import java.util.function.Function;


/**
 * Добавляет в глобальные переменные все ключи объекта(map) и атрибута setGlobalsTo
 *
 *
 */
@Step
public class SetGlobalsToStep extends BaseCompositeStep {

    public static final String SET_GLOBALS_TO = "setGlobalsTo";

    public SetGlobalsToStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return SET_GLOBALS_TO;
    }

    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal,
                                                  int level, Map<String, Object> globalVars,
                                                  Map<String, Object> specMap) {
        return x -> {
            Map<String, Object> variableAssignment = resolveAllPlaceholders(specMap, globalVars, x);
            globalVars.putAll(getTypeFromMap(variableAssignment, getStepName()));
            return Flux.just(x);
        };

    }

}