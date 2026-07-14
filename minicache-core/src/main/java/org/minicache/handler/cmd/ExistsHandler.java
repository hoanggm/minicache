package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ExistsHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static ExistsHandler handler;

    public static ExistsHandler getInstance(CacheEngine cacheEngine) {
        if (handler == null) {
            handler = new ExistsHandler(cacheEngine);
        }

        return handler;
    }

    private ExistsHandler(CacheEngine cacheEngine) {
        super(cacheEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.EXISTS.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
    }

    @Override
    public Integer handle(Message input) {
        validateInput(input);
        return cacheEngine.exists(input.getKey());
    }
}
