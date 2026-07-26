package org.minicache.common;

import org.slf4j.MDC;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class AsyncMDC {
    public static <T> Supplier<T> wrap(Supplier<T> supplier) {
        Map<String, String> contextMap = MDC.getCopyOfContextMap();
        if (contextMap == null || contextMap.isEmpty()) {
            return supplier;
        }
        return () -> {
            Map<String, String> previous = MDC.getCopyOfContextMap();
            MDC.setContextMap(contextMap);
            try {
                return supplier.get();
            } finally {
                if (previous == null) {
                    MDC.clear();
                } else {
                    MDC.setContextMap(previous);
                }
            }
        };
    }

    public static <T> Consumer<T> wrap(Consumer<T> action) {
        Map<String, String> contextMap = MDC.getCopyOfContextMap();
        if (contextMap == null || contextMap.isEmpty()) {
            return action;
        }
        return value -> {
            Map<String, String> previous = MDC.getCopyOfContextMap();
            MDC.setContextMap(contextMap);
            try {
                action.accept(value);
            } finally {
                if (previous == null) {
                    MDC.clear();
                } else {
                    MDC.setContextMap(previous);
                }
            }
        };
    }
}
