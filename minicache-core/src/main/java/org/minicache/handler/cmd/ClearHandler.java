package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ClearHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static ClearHandler handler;

    public static ClearHandler getInstance(CacheEngine cacheEngine) {
        if (handler == null) {
            handler = new ClearHandler(cacheEngine);
        }

        return handler;
    }

    private ClearHandler(CacheEngine cacheEngine) {
        super(cacheEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.CLEAR.equals(message.getCommand())) {
            throw new RuntimeException();
        }
    }

    @Override
    public Integer handle(Message input) {
        validateInput(input);
        return cacheEngine.clear(input.getCommand());
    }
}
