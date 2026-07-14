package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ZIncrHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static ZIncrHandler handler;

    public static ZIncrHandler getInstance(CacheEngine cacheEngine) {
        if (handler == null) {
            handler = new ZIncrHandler(cacheEngine);
        }

        return handler;
    }

    private ZIncrHandler(CacheEngine cacheEngine) {
        super(cacheEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.Z_INCR.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
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
    public Integer handle(Message input) {
        validateInput(input);
        return cacheEngine.zIncrBy(input.getKey(), input.getZsScore(), input.getZsMember());
    }
}
