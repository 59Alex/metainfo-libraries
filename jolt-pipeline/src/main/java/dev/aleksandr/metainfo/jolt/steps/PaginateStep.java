package dev.aleksandr.metainfo.jolt.steps;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import dev.aleksandr.metainfo.jolt.JoltPipeHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Псевдопоследовательная итерация по  данных получаемым из набора операций "source"
 * получение данных может происходить параллельно в нескольких "реактивных потоках"
 * pageNumber в результатах "source" будет заменен на индекс очередной итерации
 * useInput - true : использовать входные данные для итерации, false - использовать глобальный бизнес контекст
 */
@Step
public class PaginateStep extends BaseCompositeStep {

    public static final String PAGINATE_STEP = "paginate";
    public static final String PAGE_INCREMENT = "pageIncrement";
    public static final String GET_DATA_PAGE_NUMBER = "pageNumber";
    public static final String PAGING_KEY = "paging";
    public static final String SOURCE_KEY = "source";
    public static final String OUTPUT_ALL = "outputAll";
    public static final String OUTPUT_LAST = "outputLast";


    public PaginateStep(JoltPipeHandler process) {
        super(process);
    }

    public String getStepName() {
        return PAGINATE_STEP;
    }

    public Function<Object, Publisher<Object>> execute(SpecContext specContext, int specIdxLocal,
                                                  int level, Map<String, Object> globalVars, Map<String, Object> specMap) {
        boolean useInput = (boolean) specMap.getOrDefault(USE_INPUT, false);
        boolean outputAll = (boolean) specMap.getOrDefault(OUTPUT_ALL, false);
        boolean outputLast = (boolean) specMap.getOrDefault(OUTPUT_LAST, false);
        List<Map<String, Object>> sourceSpecs = getTypeFromMap(specMap, SOURCE_KEY);
        List<Map<String, Object>> operations = getTypeFromMap(specMap, OPERATIONS);

        Object businessContext = specContext.businessContext();
        return x -> {
            Flux<Object> paggingFlux = Flux.generate(
                            () -> 1,
                            (pageIndex, emitter) -> {
                                emitter.next(pageIndex);
                                return pageIndex + 1;
                            })
                    .concatMap(pageIndex -> {
                        Map<String, Object> bcLocal = new HashMap<>((Map) businessContext);
                        if (useInput) {
                            bcLocal = new HashMap<>((Map) x);
                        }

                        Map<String, Object> pagingRq = new HashMap<>();
                        if (!specMap.containsKey(PAGE_INCREMENT) || (specMap.containsKey(PAGE_INCREMENT) && (Boolean) specMap.get(PAGE_INCREMENT))) {
                            pagingRq.put(GET_DATA_PAGE_NUMBER, pageIndex);
                            bcLocal.put(PAGING_KEY, pagingRq);
                        }

                        return handleSpecs(Flux.just(bcLocal), sourceSpecs, globalVars,
                                specIdxLocal, level + 1, specContext);
                    }, 1)
                    .cast(List.class)
                    .takeUntil(List::isEmpty)
                    .filter(list -> !list.isEmpty())
                    .concatMap(sf ->
                                    handleSpecs(Flux.just(sf), operations, globalVars, specIdxLocal,
                                            level + 1, specContext),
                            1
                    );
            if (outputAll) {
                return paggingFlux.collectList().cast(Object.class);
            } else if (outputLast) {
                return paggingFlux.last();
            }
            return paggingFlux.then(Mono.just(x));
        };
    }


    @Override
    public Flux<Object> mapFunction(Flux<Object> fluxObject, SpecContext specContext, int specIdxLocal, int level, Map<String, Object> globalVars, Map<String, Object> stepSpecs) {
        return fluxObject.concatMap(execute(specContext, specIdxLocal, level, globalVars, stepSpecs), 1);
    }

    @Override
    protected String[] ignoreInLogs(Map<String, Object> specMap) {
        return new String[]{SOURCE_KEY, OPERATIONS};
    }

}