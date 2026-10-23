package dev.aleksandr.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Переключает выходной поток на объект из конфигурации, если в одной из передыщуих преобразований получен null или пустой список.
 */
@Step
public class SwitchIfEmptyStep extends BaseCompositeStep {

    public static final String SWITCH_IF_EMPTY_KEY = "switchIfEmpty";

    public SwitchIfEmptyStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return SWITCH_IF_EMPTY_KEY;
    }


    @Override
    protected Flux<Object> mapFunction(Flux<Object> fluxObject, SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        fluxObject = fluxObject.switchIfEmpty(Flux.defer(() -> {
                    Object anElse = specMap.get(SWITCH_IF_EMPTY_KEY);
                    return Mono.just(ifEmpty(anElse));
                })
        );
        fluxObject = fluxObject.map(x -> {
            if (x instanceof List<?> list && list.isEmpty()) {
                Object anElse = specMap.get(SWITCH_IF_EMPTY_KEY);
                return ifEmpty(anElse);
            }
            return x;
        });
        return fluxObject;
    }

    @Override
    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> zipSpecs) {
        return null;
    }

}