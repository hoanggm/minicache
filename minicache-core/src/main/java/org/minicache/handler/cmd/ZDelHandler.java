package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class ZDelHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static ZDelHandler handler;

    public static ZDelHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new ZDelHandler(storageEngine);
        }

        return handler;
    }

    private ZDelHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.Z_DEL.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
    }

    @Override
    public Integer handle(Message input) {
        validateInput(input);
        return storageEngine.zDel(input.getKey());
    }
}
