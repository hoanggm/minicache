package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ZRemHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static ZRemHandler handler;

    public static ZRemHandler getInstance(CacheEngine cacheEngine) {
        if (handler == null) {
            handler = new ZRemHandler(cacheEngine);
        }

        return handler;
    }

    private ZRemHandler(CacheEngine cacheEngine) {
        super(cacheEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.Z_RM.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getZsMember() == null || message.getZsMember().isBlank()) {
            throw new RuntimeException();
        }
    }

    @Override
    public Integer handle(Message input) {
        validateInput(input);
        return cacheEngine.zRem(input.getKey(), input.getZsMember());
    }
}
