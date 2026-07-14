package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ZDelHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static ZDelHandler handler;

    public static ZDelHandler getInstance(CacheEngine cacheEngine) {
        if (handler == null) {
            handler = new ZDelHandler(cacheEngine);
        }

        return handler;
    }

    private ZDelHandler(CacheEngine cacheEngine) {
        super(cacheEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.Z_DEL.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
    }

    @Override
    public Integer handle(Message input) {
        validateInput(input);
        return cacheEngine.zDel(input.getKey());
    }
}
