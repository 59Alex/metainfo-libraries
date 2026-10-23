package io.github.alex59.metainfo.jolt.steps;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import io.github.alex59.metainfo.jolt.JoltPipeHandler;
import io.github.alex59.metainfo.jolt.exceptions.DebugExecException;

import java.util.Map;
import java.util.function.Function;

/**
 * Останавливает исполнение и выводит максимально подробный лог
 */
@Step
public class DebugStep extends BaseCompositeStep {

    public static final String DEBUG = "debug";

    public DebugStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return DEBUG;
    }

    @Override
    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> zipSpecs) {
        return x -> {
            String inpJson;
            try {
                inpJson = defaultMapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(x);
                String ctxJson = defaultMapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(globalVars);
                specContext.log().info("\n ========DEBUG ctx======= \n{}", ctxJson);
                specContext.log().info("\n ========DEBUG INPUT======= \n{}", inpJson);
            } catch (JsonProcessingException e) {
                return Mono.error(e);
            }
            return Mono.error(new DebugExecException(inpJson));
        };
    }
}