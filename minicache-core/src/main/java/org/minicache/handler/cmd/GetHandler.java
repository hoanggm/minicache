package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class GetHandler extends BaseHandler implements ICacheHandler<String> {
    private static GetHandler handler;

    public static GetHandler getInstance(CacheEngine cacheEngine) {
        if (handler == null) {
            handler = new GetHandler(cacheEngine);
        }

        return handler;
    }

    private GetHandler(CacheEngine cacheEngine) {
        super(cacheEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.GET.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return cacheEngine.get(input.getKey());
    }
}
