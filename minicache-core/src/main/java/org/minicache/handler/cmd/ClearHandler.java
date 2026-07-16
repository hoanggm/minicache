package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ClearHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static ClearHandler handler;

    public static ClearHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new ClearHandler(storageEngine);
        }

        return handler;
    }

    private ClearHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.CLEAR.equals(message.getCommand())) {
            throw new RuntimeException();
        }
    }

    @Override
    public Integer handle(Message input) {
        validateInput(input);
        return storageEngine.clear(input.getCommand());
    }
}
