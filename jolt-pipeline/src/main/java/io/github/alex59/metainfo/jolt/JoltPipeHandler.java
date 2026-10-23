package io.github.alex59.metainfo.jolt;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.SimpleBeanDefinitionRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.core.type.filter.TypeFilter;
import reactor.core.publisher.Flux;
import io.github.alex59.metainfo.jolt.exceptions.ObsoleteResourceException;
import io.github.alex59.metainfo.jolt.model.JoltLoggingProperties;
import io.github.alex59.metainfo.jolt.model.PipeSettings;
import io.github.alex59.metainfo.jolt.steps.BaseStep;
import io.github.alex59.metainfo.jolt.steps.Step;
import io.github.alex59.metainfo.jolt.utils.SpecUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.function.Consumer;

import static io.github.alex59.metainfo.jolt.utils.SpecUtils.*;


@Slf4j
public class JoltPipeHandler {

    @Getter
    private final ApplicationContext applicationContext;
    @Getter
    private final Map<String, Object> pipeProperties;


    private final Map<String, BaseStep> steps = new HashMap<>();

    @SneakyThrows
    public JoltPipeHandler(@SuppressWarnings("Autowired") ApplicationContext applicationContext, Map<String, Object> pipeProperties) {
        this.applicationContext = applicationContext;
        this.pipeProperties = pipeProperties;

        loadSteps();
    }

    private void loadSteps() throws ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException {
        BeanDefinitionRegistry bdr = new SimpleBeanDefinitionRegistry();
        ClassPathBeanDefinitionScanner s = new ClassPathBeanDefinitionScanner(bdr);
        TypeFilter tf = new AssignableTypeFilter(BaseStep.class);
        s.addIncludeFilter(tf);
        s.scan(BaseStep.class.getPackageName());
        String[] beans = bdr.getBeanDefinitionNames();
        for (String bean : beans) {
            Class<?> cl = Class.forName(bdr.getBeanDefinition(bean).getBeanClassName());
            if (cl.isAnnotationPresent(Step.class)) {
                BaseStep step = (BaseStep) cl.getDeclaredConstructors()[0].newInstance(this);
                steps.put(step.getStepName(), step);
            }
        }
    }

    public Flux<Object> handleSpecsList(
            List<Map<String, Object>> specs,
            PipeSettings pipeSettings) {
        int parentSpecIdx = 0;
        int level = 0;
        Flux<Object> fluxObject = Flux.just(pipeSettings.businessContext());
        return handleSpecsByRef(fluxObject, specs, pipeSettings.inputGlobalVars(), pipeSettings, parentSpecIdx, level);
    }

    @SneakyThrows
    public Flux<Object> handleSpecsString(
            String jsonConfigString,
            Object businessContext,
            Map<String, Object> globalVars,
            Level logLevel,
            Logger reactContextLogger,
            Consumer<? super Object> isPipeStopped
    ) {
        jsonConfigString = replaceVariables(jsonConfigString, globalVars);
        List<Map<String, Object>> specs = parseConfig(jsonConfigString);
        PipeSettings pipeSettings = new PipeSettings(businessContext, Map.of(),
                new JoltLoggingProperties(true, logLevel), globalVars, isPipeStopped, reactContextLogger);
        return handleSpecsList(specs, pipeSettings);
    }

    @SneakyThrows
    public Flux<Object> handleSpecsString(
            String jsonConfigString,
            Object businessContext,
            Map<String, Object> globalVars,
            Level logLevel,
            Logger reactContextLogger) {

        return handleSpecsString(jsonConfigString, businessContext,
                globalVars, logLevel, reactContextLogger, o -> {
                });
    }


    private Flux<Object> handleSpecsByRef(Flux<Object> fluxObject,
                                          List<Map<String, Object>> specs,
                                          Map<String, Object> globalVars, PipeSettings pipeSettings, int parentSpecIdx, int level) {

        // обход шагов трансформации
        int specIdx = 0;
        for (Map<String, Object> specMap : specs) {
            fluxObject = fluxObject.doOnNext(pipeSettings.isPipeStopped());
            Set<String> specAttrs = specMap.keySet();
            BaseStep step = null;
            for (String specAttr : specAttrs) {
                if (steps.containsKey(specAttr)) {
                    step = steps.get(specAttr);
                    break;
                }
            }
            if (step != null) {
                BaseStep.SpecContext specContext = new BaseStep.SpecContext(specMap, pipeSettings);
                fluxObject = step.applyStep(fluxObject, specMap, specContext, specIdx, level, parentSpecIdx, globalVars);
            } else {
                throw new ObsoleteResourceException(specMap.toString());
            }
        }
        return fluxObject;
    }


    public Flux<Object> handleSpecs(Flux<Object> fluxObject,
                                    List<Map<String, Object>> specs,
                                    Map<String, Object> globalVarsIn,
                                    int parentSpecIdx, int level,
                                    BaseStep.SpecContext specContext) {
        Map<String, Object> globalVars = new HashMap<>(Optional
                .ofNullable(globalVarsIn).orElse(new HashMap<>()));
        List<Map<String, Object>> specsCopy = SpecUtils.copyObject(specs);

        return handleSpecsByRef(fluxObject, specsCopy, globalVars, specContext.pipeSettings(),
                parentSpecIdx, level);
    }

}
