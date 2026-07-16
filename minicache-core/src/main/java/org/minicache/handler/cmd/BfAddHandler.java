package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class BfAddHandler extends BaseHandler implements ICacheHandler<String> {
    private static BfAddHandler handler;

    public static BfAddHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new BfAddHandler(storageEngine);
        }

        return handler;
    }

    private BfAddHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.BF_ADD.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getValue() == null || message.getValue().isBlank()) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return storageEngine.addBloomFilter(input.getKey(), input.getValue());
    }
}
