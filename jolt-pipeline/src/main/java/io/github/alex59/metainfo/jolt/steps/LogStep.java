package io.github.alex59.metainfo.jolt.steps;

import org.slf4j.event.Level;
import reactor.core.publisher.Flux;
import io.github.alex59.metainfo.jolt.JoltPipeHandler;

import java.util.Map;

/**
 * Записывает в лог сообщение, обрабатывая выражения ${} и @{}
 */
@Step
public class LogStep extends BaseStep {

    public static final String LOG_STEP = "log";
    public static final String LEVEL = "logLevel";

    public LogStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return LOG_STEP;
    }

    @Override
    public Flux<Object> apply(Flux<Object> fluxObject, Map<String, Object> specMap, SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars) {
        String levelStr = (String) specMap.getOrDefault(LEVEL, Level.INFO.name());
        Level logLevel = Level.valueOf(levelStr.toUpperCase());

        return fluxObject.doOnNext(x -> {
            Map<String, Object> enrichedSpec = resolveAllPlaceholders(specMap, globalVars, x);
            String logMessage = getTypeFromMap(enrichedSpec, LOG_STEP);
            specContext.log().atLevel(logLevel).log(logMessage);
        });
    }

}