package io.github.alex59.metainfo.jolt.transform;

import lombok.AllArgsConstructor;
import org.slf4j.Logger;

import java.util.Map;

import static io.github.alex59.metainfo.jolt.steps.BaseStep.LOG;

/**
 * Предоставляет упрощенные методы получения данных из контекста
 */
@AllArgsConstructor
public class OperationContext {

    public static final String GLOBAL_VARIABLES = "globalVars";
    public static final String TECH_CONTEXT = "_techContext";

    private final Map<String, Object> context;

    public Map<String, Object> getContext() {
        return context;
    }

    @SuppressWarnings("unchecked")
    public <T> T getOrDefault(String key, T defaultValue) {
        return (T) context.getOrDefault(key, defaultValue);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) context.get(key);
    }

    public void remove(String key) {
        context.remove(key);
    }

    public <T> T getGlobalVars() {
        return this.get(GLOBAL_VARIABLES);
    }

    @SuppressWarnings("unchecked")
    public <T> T getGlobalVar(String key) {
        Map<String, Object> vars = this.get(GLOBAL_VARIABLES);
        return (T) vars.get(key);
    }

    @SuppressWarnings("unchecked")
    public <T> T getTechCtxVar(String key) {
        Map<String, Object> techCtx = getTechCtx();
        if (techCtx == null) {
            return null;
        }
        return (T) techCtx.get(key);
    }

    public void putTechCtxVar(String key, Object value) {
        Map<String, Object> techCtx = getTechCtx();
        if (techCtx != null) {
            techCtx.put(key, value);
        }
    }

    public void removeTechCtxVar(String key) {
        Map<String, Object> techCtx = getTechCtx();
        if (techCtx != null) {
            techCtx.remove(key);
        }
    }

    private Map<String, Object> getTechCtx() {
        return this.get(TECH_CONTEXT);
    }

    public Logger getReactiveLogger() {
        return this.get(LOG);
    }

    public void putGlobalVar(String key, Object value) {
        Map<String, Object> vars = this.get(GLOBAL_VARIABLES);
        vars.put(key, value);
    }

    public void put(String key, Object value) {
        context.put(key, value);
    }
}
