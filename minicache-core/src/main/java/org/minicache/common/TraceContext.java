package org.minicache.common;

import org.slf4j.MDC;

import java.util.UUID;

public class TraceContext {
    public static final String TRACE_ID_KEY = "traceId";

    public static String generateTraceId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    public static void setTraceId(String traceId) {
        MDC.put(TRACE_ID_KEY, traceId);
    }

    public static void clear() {
        MDC.remove(TRACE_ID_KEY);
    }
}
