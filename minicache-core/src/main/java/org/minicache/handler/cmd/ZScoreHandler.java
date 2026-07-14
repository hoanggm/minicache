package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ZScoreHandler extends BaseHandler implements ICacheHandler<String> {
    private static ZScoreHandler handler;

    public static ZScoreHandler getInstance(CacheEngine cacheEngine) {
        if (handler == null) {
            handler = new ZScoreHandler(cacheEngine);
        }

        return handler;
    }

    private ZScoreHandler(CacheEngine cacheEngine) {
        super(cacheEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.Z_SCR.equals(message.getCommand())) {
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
    public String handle(Message input) {
        validateInput(input);
        return cacheEngine.zScore(input.getKey(), input.getZsMember());
    }
}
