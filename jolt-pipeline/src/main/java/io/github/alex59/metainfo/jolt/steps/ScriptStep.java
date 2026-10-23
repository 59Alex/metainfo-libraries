package io.github.alex59.metainfo.jolt.steps;

import lombok.SneakyThrows;
import org.codehaus.groovy.jsr223.GroovyScriptEngineFactory;
import io.github.alex59.metainfo.jolt.JoltPipeHandler;
import io.github.alex59.metainfo.jolt.exceptions.JoltExtBusinessException;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.Arrays;
import java.util.Map;

import static io.github.alex59.metainfo.jolt.transform.OperationContext.GLOBAL_VARIABLES;


/**
 * Исполняет динамический скрипт на Groovy из атрибута "script"
 * "input" - входящий объект
 * "transformContextLocal" - настройка шаг
 * "globalVars" - глобальные переменные
 */
@Step
public class ScriptStep extends OperationStep {

    public static final String TRANSFORM_CONTEXT_LOCAL_KEY = "transformContextLocal";

    public static final String INPUT_KEY = "input";

    public static final String SCRIPT_KEY = "script";

    public ScriptStep(JoltPipeHandler process) {
        super(process);
    }

    @Override
    public String getStepName() {
        return SCRIPT_KEY;
    }

    @Override
    protected Object transform(Map<String, Object> transformContextLocal, Object x, Map<String, Object> specMapLocal) {
        return executeScript(specMapLocal, transformContextLocal, x);
    }

    @SneakyThrows
    private Object executeScript(Map<String, Object> specMap, Map<String, Object> transformContextLocal, Object x) {
        String script = specMap.get(SCRIPT_KEY).toString();
        SimpleBindings simpleBindings = new SimpleBindings();
        simpleBindings.put(TRANSFORM_CONTEXT_LOCAL_KEY, transformContextLocal);
        simpleBindings.put(GLOBAL_VARIABLES, transformContextLocal.get(GLOBAL_VARIABLES));
        simpleBindings.put(INPUT_KEY, x);
        ScriptEngine scriptEngine = new GroovyScriptEngineFactory().getScriptEngine();
        try {
            return scriptEngine.eval(script, simpleBindings);
        } catch (javax.script.ScriptException e) {
            // Workaround нет строки ошибки в исключении
            Throwable cause = rootCause(e);
            if (cause instanceof JoltExtBusinessException businessException) {
                throw businessException;
            }
            int lineNumber = Arrays.stream(cause.getStackTrace())
                    .filter(it -> it.getFileName() != null && it.getFileName().matches("^Script.*groovy$"))
                    .findFirst().orElseThrow().getLineNumber();
            throw new ScriptException(cause.getMessage(), "Script", lineNumber);
        }
    }

    private Throwable rootCause(Exception e) {
        Throwable t = e;
        while (t.getCause() != null) t = t.getCause();
        return t;
    }

}