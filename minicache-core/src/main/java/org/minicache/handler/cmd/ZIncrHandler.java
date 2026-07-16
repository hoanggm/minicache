package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ZIncrHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static ZIncrHandler handler;

    public static ZIncrHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new ZIncrHandler(storageEngine);
        }

        return handler;
    }

    private ZIncrHandler(StorageEngine storageEngine) {
        super(storageEngine);
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
        return storageEngine.zIncrBy(input.getKey(), input.getZsScore(), input.getZsMember());
    }
}
