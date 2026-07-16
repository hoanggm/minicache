package org.minicache.handler;

import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;

public abstract class BaseHandler {
    protected final StorageEngine storageEngine;

    protected BaseHandler(StorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    protected abstract void validateInput(Message message);
}
