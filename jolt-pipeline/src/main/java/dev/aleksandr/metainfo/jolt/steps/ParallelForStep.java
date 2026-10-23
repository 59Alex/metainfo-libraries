package dev.aleksandr.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.util.concurrent.Queues;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Параллельное выполнение шагов c настройкой количества потоков concurrency
 */
@Step
public class ParallelForStep extends BaseCompositeStep {

    public static final String CONCURRENCY_KEY = "concurrency";
    public static final String FOR_PAR = "parallelFor";
    public static final String OUTPUT_ALL = "outputAll";

    public ParallelForStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return FOR_PAR;
    }

    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal,
                                                       int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        int concurrency = (int) specMap.getOrDefault(CONCURRENCY_KEY, Queues.SMALL_BUFFER_SIZE);
        boolean outputAll = (boolean) specMap.getOrDefault(OUTPUT_ALL, false);
        List<Map<String, Object>> childSpecs = getTypeFromMap(specMap, getStepName());
        return x -> {
            Flux<Object> objectFlux = Flux.fromIterable((List<Object>) x).flatMap(element ->
                    handleSpecs(Flux.just(element), childSpecs, globalVars,
                            specIdxLocal, level + 1, specContext), concurrency);
            if (!outputAll) {
                objectFlux = objectFlux
                        .count().flux().cast(Object.class);
            } else {
                objectFlux = objectFlux.collectList().flux().cast(Object.class);
            }
            return objectFlux;
        };
    }

    @Override
    public Flux<Object> mapFunction(Flux<Object> fluxObject, SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> stepSpecs) {
        return fluxObject.concatMap(execute(specContext, specIdxLocal, level, globalVars, stepSpecs));
    }

}