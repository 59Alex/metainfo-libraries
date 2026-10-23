package io.github.alex59.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import io.github.alex59.metainfo.jolt.JoltPipeHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Исполняет набор шагов, без влияния на выходные данные
 * Тоже, что @{GroupStep}, но с без влияния на выходные данные
 */
@Step
public class SideStep extends BaseCompositeStep {

    public static final String SIDE_STEP = "sideEffect";

    public SideStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return SIDE_STEP;
    }

    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal,
                                                  int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        List<Map<String, Object>> childSpecs = getTypeFromMap(specMap, getStepName());
        return x -> handleSpecs(Flux.just(x), childSpecs, globalVars,
                specIdxLocal, level + 1, specContext)
                .then(Mono.just(x));
    }

}