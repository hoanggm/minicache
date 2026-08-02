package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

import java.util.concurrent.CompletableFuture;

public class HRmHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static HRmHandler handler;

    public static HRmHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new HRmHandler(storageEngine);
        }

        return handler;
    }

    private HRmHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.H_RM.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getHsField() == null || message.getHsField().isBlank()) {
            throw new RuntimeException();
        }
    }

    @Override
    public Integer handle(Message input) {
        validateInput(input);
        return storageEngine.hRm(input.getKey(), input.getHsField());
    }

    @Override
    public CompletableFuture<Integer> handleAsync(Message input) {
        validateInput(input);
        return storageEngine.hRmAsync(input.getKey(), input.getHsField());
    }
}
