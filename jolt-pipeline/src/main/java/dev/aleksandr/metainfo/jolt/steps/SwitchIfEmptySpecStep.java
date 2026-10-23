package dev.aleksandr.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


/**
 * Исполняет вложенные трансформы в случае пустого списка или null в любом из предыдущих преобразований.
 * Иначе будет пропущен
 */
@Step
public class SwitchIfEmptySpecStep extends BaseCompositeStep {

    public static final String SWITCH_IF_EMPTY_KEY = "switchIfEmptySpec";

    public SwitchIfEmptySpecStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return SWITCH_IF_EMPTY_KEY;
    }


    @Override
    protected Flux<Object> mapFunction(Flux<Object> fluxObject, SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> specMap) {

        List<Map<String, Object>> operations = getTypeFromMap(specMap, SWITCH_IF_EMPTY_KEY);

        fluxObject = fluxObject.flatMap(x -> {
            if (x instanceof List<?> list && list.isEmpty()) {
                return Mono.empty();
            }
            return Mono.just(x);
        });

        fluxObject = fluxObject.switchIfEmpty(Flux.defer(() ->
                handleSpecs(Flux.just(new ArrayList<>()), operations, globalVars,
                        specIdxLocal, level + 1, specContext))
        );

        return fluxObject;
    }

    @Override
    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> zipSpecs) {
        return null;
    }

}