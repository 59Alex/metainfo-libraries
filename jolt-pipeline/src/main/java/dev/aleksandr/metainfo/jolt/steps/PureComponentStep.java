package dev.aleksandr.metainfo.jolt.steps;

import lombok.SneakyThrows;
import org.reactivestreams.Publisher;
import org.springframework.util.StreamUtils;
import reactor.core.publisher.Flux;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;
import dev.aleksandr.metainfo.jolt.utils.SpecUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static dev.aleksandr.metainfo.jolt.steps.OperationStep.ENRICH_FROM_CONTEXT;
import static dev.aleksandr.metainfo.jolt.utils.SpecUtils.*;

/**
 * Переиспользование набора шагов с параметрами
 */
@Step
public class PureComponentStep extends BaseCompositeStep {

    public static final String PURE_COMPONENT = "component";
    public static final String PARAMS_KEY = "params";
    public static final String JSON_CONFIGS_COMMON_COMPONENTS_JSON = "jsonConfigs/common/components.json";
    public static final String JOLT = "jolt";
    private final Map<String, List<Map<String, Object>>> componentsMap;

    public PureComponentStep(JoltPipeHandler process) {
        super(process);
        this.componentsMap = getComponentsMap(process.getPipeProperties());
    }

    public String getStepName() {
        return PURE_COMPONENT;
    }

    @Override
    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        return x -> {
            Map<String, Object> specMapLocal = new HashMap<>(specMap);
            Map<String, Object> variables = new HashMap<>(globalVars);
            if (specMapLocal.containsKey(ENRICH_FROM_CONTEXT)) {
                specMapLocal = SpecUtils.resolvePlaceHolders(specMap, variables);
            }
            Map<String, Object> params = getTypeFromMap(specMapLocal, PARAMS_KEY);
            if (params != null) {
                variables.putAll(params);
            }
            return applyComponent(String.valueOf(specMapLocal.get(PURE_COMPONENT)),
                    Flux.just(x), variables, specIdxLocal, level + 1, specContext);
        };
    }

    @SneakyThrows
    private Map<String, List<Map<String, Object>>> getComponentsMap(Map<String, Object> joltEnv) {
        String className = (String) joltEnv.get("joltConfigStoreClass");
        if (className != null) {
            ClassLoader resourceclassLoader = Class.forName(className).getClassLoader();
            String jsonStringConfig = StreamUtils.copyToString(
                    resourceclassLoader.getResourceAsStream(JSON_CONFIGS_COMMON_COMPONENTS_JSON),
                    StandardCharsets.UTF_8);
            jsonStringConfig = replaceVariables(jsonStringConfig, getTypeFromMap(joltEnv, JOLT));
            return parseConfig(jsonStringConfig);
        } else {
            return new HashMap<>();
        }
    }

    public Flux<Object> applyComponent(String componentName, Flux<Object> input,
                                       Map<String, Object> globalVars,
                                       int parentSpecIdx, int level, SpecContext specContext) {
        List<Map<String, Object>> componentTransforms = copyObject(componentsMap.get(componentName));
        return process.handleSpecs(input, componentTransforms, globalVars,
                parentSpecIdx, level, specContext);
    }

    @Override
    protected String[] ignoreInLogs(Map<String, Object> specMap) {
        return new String[0];
    }
}