package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ZRanScoreHandler extends BaseHandler implements ICacheHandler<String> {
    private static ZRanScoreHandler handler;

    public static ZRanScoreHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new ZRanScoreHandler(storageEngine);
        }

        return handler;
    }

    private ZRanScoreHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.Z_RSCR.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getZsStartScr() == null) {
            throw new RuntimeException();
        }
        if (message.getZsStopScr() == null) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return storageEngine.zRangeByScore(input.getKey(), input.getZsStartScr(), input.getZsStopScr());
    }
}
