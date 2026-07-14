package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class BfAddHandler extends BaseHandler implements ICacheHandler<String> {
    private static BfAddHandler handler;

    public static BfAddHandler getInstance(CacheEngine cacheEngine) {
        if (handler == null) {
            handler = new BfAddHandler(cacheEngine);
        }

        return handler;
    }

    private BfAddHandler(CacheEngine cacheEngine) {
        super(cacheEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.BF_ADD.equals(message.getCommand())) {
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
        validateInput(input);
        return cacheEngine.addBloomFilter(input.getKey(), input.getValue());
    }
}
