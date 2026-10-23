package dev.aleksandr.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;
import dev.aleksandr.metainfo.jolt.utils.SpecUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Collections.emptyList;

/**
 * Выполняет вложенные шаги из if если на вход пришел не пустой список, иначе выполняет шаги else
 */
@Step
public class IfStep extends BaseCompositeStep {
    public static final String IF_KEY = "if";
    public static final String ELSE_KEY = "else";

    public IfStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return IF_KEY;
    }


    @Override
    protected Flux<Object> mapFunction(Flux<Object> fluxObject, SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        List<Map<String, Object>> ifSpecs = (List<Map<String, Object>>) specMap.get(IF_KEY);
        String varInput = getVarInput(specMap);
        List<Map<String, Object>> elseSpecs = (List<Map<String, Object>>) specMap.get(ELSE_KEY);
        List<Map<String, Object>> operations = (List<Map<String, Object>>) specMap.get(OPERATIONS);
        boolean useInput = (boolean) specMap.getOrDefault(USE_INPUT, false);

        return fluxObject.flatMap(x ->
                ((varInput != null) ?
                        Flux.just(
                                Optional.ofNullable(
                                                SpecUtils.resolvePlaceHolders(varInput, globalVars))
                                        .map(List::of)
                                        .orElse(emptyList()))
                        : handleSpecs(Flux.just(x), ifSpecs, globalVars,
                        specIdxLocal, level + 1, specContext))

                        .flatMap(y -> {
                            Flux<Object> inpX = useInput ? Flux.just(SpecUtils.copyObject(y)) : Flux.just(x);
                            if (y instanceof List<?> list && !list.isEmpty()) {
                                return handleSpecs(inpX, operations, globalVars,
                                        specIdxLocal, level + 1, specContext);
                            } else {
                                if (elseSpecs == null) {
                                    return inpX;
                                }
                                return handleSpecs(inpX, elseSpecs, globalVars,
                                        specIdxLocal, level + 1, specContext);
                            }
                        }).switchIfEmpty(Mono.just(x)));


    }

    @Override
    protected String[] ignoreInLogs(Map<String, Object> specMap) {
        return new String[]{ELSE_KEY, OPERATIONS};
    }

    protected String getVarInput(Map<String, Object> specMap) {
        return null;
    }

    @Override
    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> zipSpecs) {
        return null;
    }

}