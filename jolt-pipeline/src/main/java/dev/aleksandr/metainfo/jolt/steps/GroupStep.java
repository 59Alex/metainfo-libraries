package dev.aleksandr.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Просто группировка шагов без влияния на логику и результат
 */
@Step
public class GroupStep extends BaseCompositeStep {

    public static final String GROUP_STEP = "group";

    public GroupStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return GROUP_STEP;
    }

    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal,
                                                  int level, Map<String, Object> globalVars, Map<String, Object> specMap) {

        List<Map<String, Object>> groupSpecs = getTypeFromMap(specMap, GROUP_STEP);

        return x -> handleSpecs(Flux.just(x), groupSpecs, globalVars,
                specIdxLocal, level + 1, specContext);
    }

}