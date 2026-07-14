package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ZRankHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static ZRankHandler handler;

    public static ZRankHandler getInstance(CacheEngine cacheEngine) {
        if (handler == null) {
            handler = new ZRankHandler(cacheEngine);
        }

        return handler;
    }

    private ZRankHandler(CacheEngine cacheEngine) {
        super(cacheEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.Z_RANK.equals(message.getCommand())) {
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
        return cacheEngine.zRank(input.getKey(), input.getZsMember());
    }
}
