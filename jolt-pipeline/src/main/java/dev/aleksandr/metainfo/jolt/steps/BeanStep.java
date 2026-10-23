package dev.aleksandr.metainfo.jolt.steps;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.springframework.context.ApplicationContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;
import dev.aleksandr.metainfo.jolt.model.JoltLoggingProperties;
import dev.aleksandr.metainfo.jolt.transform.NoBlockContextualTransformExt;

import java.util.HashMap;
import java.util.Map;


/**
 * Шаг позволяет вызвать spring bean,
 * в отличие от  {@link NoBlockContextualTransformExt} не создает класс и берет его из контекста
 */
@Step
public class BeanStep extends OperationStep {

    public static final String BEAN_SPEC = "bean";
    private final ApplicationContext applicationContext;

    public BeanStep(JoltPipeHandler process) {
        super(process);
        this.applicationContext = process.getApplicationContext();
    }

    @Override
    public String getStepName() {
        return BEAN_SPEC;
    }

    @Override
    protected Flux<Object> prepareTransform(Flux<Object> fluxObject, Map<String, Object> globalVars, Map<String, Object> specMap, Map<String, Object> transformContextLocal, Logger reactContextLogger, JoltLoggingProperties logging, String comment, Object businessContext) {
        return fluxObject.flatMap(x -> {
            NoBlockContextualTransformExt<?> bean;
            try {
                String className = specMap.get(BEAN_SPEC).toString();
                bean = getBeanTransform(className);
            } catch (ClassNotFoundException e) {
                return Mono.error(e);
            }

            final Map<String, Object> specMapLocal = new HashMap<>(specMap);
            if (specMapLocal.containsKey(ENRICH_FROM_CONTEXT)) {
                transformContextLocal.putAll((Map<String, Object>) specMapLocal.computeIfPresent(JOLT_SPEC_KEY,
                        (key, joltSpec) ->
                                enrichFromContext((Map<String, Object>) joltSpec, transformContextLocal)));
            }
            return springBeanStep(bean, transformContextLocal, x)
                    .switchIfEmpty(
                            Mono.fromCallable(() -> handleTransformResult(comment, specMapLocal, null, logging, reactContextLogger))
                                    .flux()
                    )
                    .onErrorResume(
                            handleTransformError(specMapLocal, transformContextLocal, comment, logging, x, reactContextLogger)
                    );
        });
    }

    /**
     * @param transformContext - глобальный контекст трансформации
     * @param x                - входящий объект
     * @return поток объектов
     */
    @SneakyThrows
    private Flux<Object> springBeanStep(NoBlockContextualTransformExt<?> bean, Map<String, Object> transformContext,
                                        Object x) {
        return bean.transform(x, transformContext);
    }

    public NoBlockContextualTransformExt<Object> getBeanTransform(String className) throws ClassNotFoundException {
        return (NoBlockContextualTransformExt<Object>) applicationContext.getBean(Class.forName(className));
    }
}