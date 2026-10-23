package io.github.alex59.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import io.github.alex59.metainfo.jolt.JoltPipeHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 *  Склеивает входной поток и данные полученные при обработке вложенной трансформации в Map
 */
@Step
public class ZipMapStep extends BaseCompositeStep {

    public static final String MAIN_KEY = "main";
    public static final String ZIPPED_KEY = "zipped";


    public static final String ZIP_MAP_STEP = "zipMap";

    public ZipMapStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return ZIP_MAP_STEP;
    }

    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal,
                                                  int level, Map<String, Object> globalVars,
                                                  Map<String, Object> specMap) {

        List<Map<String, Object>> zipSpecs = getTypeFromMap(specMap, getStepName());
        return x ->
                Flux.zip(Flux.just(x), handleSpecs(Flux.just(x), zipSpecs, globalVars,
                                specIdxLocal, level + 1, specContext),
                        (k1, v1) -> Map.of(MAIN_KEY, k1, ZIPPED_KEY, v1));
    }

    @Override
    protected Flux<Object> mapFunction(Flux<Object> fluxObject, SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        return fluxObject.concatMap(execute(specContext, specIdxLocal, level, globalVars, specMap), 1);
    }

}