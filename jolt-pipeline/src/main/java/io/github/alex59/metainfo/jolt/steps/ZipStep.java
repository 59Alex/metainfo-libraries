package io.github.alex59.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import io.github.alex59.metainfo.jolt.JoltPipeHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 *  Склеивает входной поток и данные полученные при обработке вложенной трансформации в Pair
 *
 *  @deprecated use {@link io.github.alex59.metainfo.jolt.steps.ZipMapStep}
 */
@Deprecated(since="1.0", forRemoval=true)
@Step
public class ZipStep extends BaseCompositeStep {

    public static final String ZIP_STEP = "zip";

    public ZipStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return ZIP_STEP;
    }

    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal,
                                                  int level, Map<String, Object> globalVars,
                                                  Map<String, Object> specMap) {
        List<Map<String, Object>> childSpecs = getTypeFromMap(specMap, getStepName());
        return x ->
                Flux.zip(Flux.just(x), handleSpecs(Flux.just(x), childSpecs, globalVars,
                        specIdxLocal,
                        level + 1, specContext)).cast(Object.class);
    }

}