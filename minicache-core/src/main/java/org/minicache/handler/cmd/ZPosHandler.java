package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ZPosHandler extends BaseHandler implements ICacheHandler<String> {
    private static ZPosHandler handler;

    public static ZPosHandler getInstance(CacheEngine cacheEngine) {
        if (handler == null) {
            handler = new ZPosHandler(cacheEngine);
        }

        return handler;
    }

    private ZPosHandler(CacheEngine cacheEngine) {
        super(cacheEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.Z_POS.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getZsIdx() == null) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return cacheEngine.zGetByPosition(input.getKey(), input.getZsIdx());
    }
}
