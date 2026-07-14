package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class BfResetHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static BfResetHandler handler;

    public static BfResetHandler getInstance(CacheEngine cacheEngine) {
        if (handler == null) {
            handler = new BfResetHandler(cacheEngine);
        }

        return handler;
    }

    private BfResetHandler(CacheEngine cacheEngine) {
        super(cacheEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.BF_RS.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
    }

    @Override
    public Integer handle(Message input) {
        validateInput(input);
        return cacheEngine.resetBloomFilter(input.getKey());
    }
}
