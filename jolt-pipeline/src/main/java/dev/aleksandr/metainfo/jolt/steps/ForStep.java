package dev.aleksandr.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Исполняет вложенные шаги последовательно для каждого элемента списка входящего списка
 */
@Step
public class ForStep extends BaseCompositeStep {

    public static final String FOR = "for";

    public ForStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return FOR;
    }

    @SuppressWarnings("unchecked")
    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal,
                                                  int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        List<Map<String, Object>> childSpecs = getTypeFromMap(specMap, getStepName());
        return x -> Flux.fromIterable((List<Object>) x).concatMap(element ->
                        handleSpecs(Flux.just(element), childSpecs, globalVars,
                                specIdxLocal, level + 1, specContext), 1)
                .count().flux().cast(Object.class);
    }

    @Override
    public Flux<Object> mapFunction(Flux<Object> fluxObject, SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> stepSpec) {
        // concatMap и prefetch 1 для последовательного выполнения
        return fluxObject.concatMap(execute(specContext, specIdxLocal, level, globalVars, stepSpec), 1);
    }
}