package io.github.alex59.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import io.github.alex59.metainfo.jolt.JoltPipeHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Исполняет вложенные шаги и пытается поместить результат в глобальные переменные
 * Результат должен иметь тип Map
 */
@Step
public class EvalToGlobalStep extends BaseCompositeStep {

    public static final String EVAL_TO_GLOBAL_VARS = "evalToGlobalVars";

    public EvalToGlobalStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return EVAL_TO_GLOBAL_VARS;
    }

    @SuppressWarnings("unchecked")
    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal,
                                                  int level, Map<String, Object> globalVars,
                                                  Map<String, Object> specMap) {

        List<Map<String, Object>> groupSpecs = getTypeFromMap(specMap, getStepName());
        return x ->
                handleSpecs(Flux.just(x), groupSpecs, globalVars,
                        specIdxLocal, level + 1, specContext)
                        .doOnNext(y -> globalVars.putAll((Map<String, Object>) y)).then(Mono.just(x));
    }

}