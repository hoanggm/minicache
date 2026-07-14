package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ZAddHandler extends BaseHandler implements ICacheHandler<String> {
    private static ZAddHandler handler;

    public static ZAddHandler getInstance(CacheEngine cacheEngine) {
        if (handler == null) {
            handler = new ZAddHandler(cacheEngine);
        }

        return handler;
    }

    private ZAddHandler(CacheEngine cacheEngine) {
        super(cacheEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.Z_ADD.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getValue() == null || message.getValue().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getZsMember() == null || message.getZsMember().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getZsScore() == null) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return cacheEngine.zAdd(input.getKey(), input.getZsScore(), input.getZsMember(), input.getValue());
    }
}
