package org.minicache.handler;

import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;

public abstract class BaseHandler {
    protected final CacheEngine cacheEngine;

    protected BaseHandler(CacheEngine cacheEngine) {
        this.cacheEngine = cacheEngine;
    }

    protected abstract void validateInput(Message message);
}
