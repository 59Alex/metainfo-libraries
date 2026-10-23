package dev.aleksandr.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;


/**
 * Игнорировать ошибки во вложенных трансформациях
 */
@Step
public class SkipErrorsStep extends BaseCompositeStep {


    public static final String EXECUTE_IGNORE_ERRORS = "executeIgnoreErrors";

    public SkipErrorsStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return EXECUTE_IGNORE_ERRORS;
    }

    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal,
                                                  int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        List<Map<String, Object>> childSpecs = getTypeFromMap(specMap, getStepName());
        return x -> handleSpecs(Flux.just(x), childSpecs, globalVars,
                specIdxLocal, level + 1, specContext)
                .onErrorReturn(x);
    }

}