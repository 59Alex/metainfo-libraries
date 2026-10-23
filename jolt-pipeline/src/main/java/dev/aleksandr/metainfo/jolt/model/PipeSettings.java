package dev.aleksandr.metainfo.jolt.model;


import org.slf4j.Logger;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Настройки текущего исполнения pipe
 * @param businessContext - входящий в пайп объект
 * @param logProps - параметры логирования
 * @param inputGlobalVars - глобальные переменные на вход
 * @param isPipeStopped - функция проверки остановки пайпа
 * @param reactContextLogger - логер с контекстом react
 */
public record PipeSettings(Object businessContext,
                           Object techContext,
                           JoltLoggingProperties logProps,
                           Map<String, Object> inputGlobalVars,
                           Consumer<? super Object> isPipeStopped,
                           Logger reactContextLogger) {
}
