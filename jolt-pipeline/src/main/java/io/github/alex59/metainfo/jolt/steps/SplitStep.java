package io.github.alex59.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import io.github.alex59.metainfo.jolt.JoltPipeHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Передает входные данные в n вложенных потоков затем собирает результат в один поток списком
 * Атрибут содержащий вложенные наборы преобразований должен содержать постфикс "stream"
 */
@Step
public class SplitStep extends BaseCompositeStep {
    public static final String PARALLEL_STEP = "split";
    public static final String STREAM_PREFIX = "stream";

    public SplitStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return PARALLEL_STEP;
    }

    @Override
    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        return x -> {
            List<List<Map<String, Object>>> streams = specMap.entrySet().stream()
                    .filter(entry -> entry.getKey().toLowerCase().contains(STREAM_PREFIX))
                    .map(entry -> (List<Map<String, Object>>) entry.getValue()).toList();
            List<Flux<Object>> specsFluxes = streams.stream().map(streamSpec ->
                            handleSpecs(Flux.just(x), streamSpec, new HashMap<>(globalVars),
                                    specIdxLocal, level + 1, specContext)
                                    .flatMapIterable(it -> (it instanceof List) ? (List<?>) it : List.of(it))
                                    .cast(Object.class))
                    .toList();
            return Flux.concat(specsFluxes).collectList().cast(Object.class);
        };
    }

    @Override
    protected String[] ignoreInLogs(Map<String, Object> specMap) {
        return specMap.keySet().stream()
                .filter(key -> key.toLowerCase().contains(STREAM_PREFIX)).toArray(String[]::new);
    }
}