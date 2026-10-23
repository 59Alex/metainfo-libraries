package dev.aleksandr.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Разбиения входящего списка на части с применением шагов к кажоой отдельно
 */
@Step
public class BufferStep extends BaseCompositeStep {

    public static final String CONCURRENCY_KEY = "concurrency";
    public static final String BUFFER_KEY = "buffer";
    public static final String CHUNK_SIZE = "chuckSize";

    public BufferStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return BUFFER_KEY;
    }

    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal,
                                                       int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        int concurrency = (int) specMap.getOrDefault(CONCURRENCY_KEY, 1);
        int chuckSize = (int) specMap.getOrDefault(CHUNK_SIZE, 10);
        List<Map<String, Object>> childSpecs = getTypeFromMap(specMap, getStepName());
        return x -> Flux.fromIterable((List<Object>) x).buffer(chuckSize)
                .concatMap(element ->
                        handleSpecs(Flux.just(element), childSpecs, globalVars,
                                specIdxLocal, level + 1, specContext), concurrency)
                .concatMapIterable(sublist -> (List<Object>) sublist, 1).collectList().cast(Object.class).flux();
    }

    @Override
    public Flux<Object> mapFunction(Flux<Object> fluxObject, SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> stepSpecs) {
        return fluxObject.concatMap(execute(specContext, specIdxLocal, level, globalVars, stepSpecs));
    }

}