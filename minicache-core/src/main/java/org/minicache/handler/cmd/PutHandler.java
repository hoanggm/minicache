package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class PutHandler extends BaseHandler implements ICacheHandler<String> {
    private static PutHandler handler;

    public static PutHandler getInstance(CacheEngine cacheEngine) {
        if (handler == null) {
            handler = new PutHandler(cacheEngine);
        }

        return handler;
    }

    private PutHandler(CacheEngine cacheEngine) {
        super(cacheEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.PUT.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getValue() == null || message.getValue().isBlank()) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        return cacheEngine.put(input.getKey(), input.getValue(), input.getTtl(), input.getNotExists());
    }
}
