package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ZRanHandler extends BaseHandler implements ICacheHandler<String> {
    private static ZRanHandler handler;

    public static ZRanHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new ZRanHandler(storageEngine);
        }

        return handler;
    }

    private ZRanHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.Z_RANGE.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getZsStartIdx() == null) {
            throw new RuntimeException();
        }
        if (message.getZsStopIdx() == null) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return storageEngine.zRangeByPositions(input.getKey(), input.getZsStartIdx(), input.getZsStopIdx());
    }
}
