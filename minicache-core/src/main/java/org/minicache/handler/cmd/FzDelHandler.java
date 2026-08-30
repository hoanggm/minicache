package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

import java.util.concurrent.CompletableFuture;

public class FzDelHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static FzDelHandler handler;

    public static FzDelHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new FzDelHandler(storageEngine);
        }

        return handler;
    }

    private FzDelHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.FZ_DEL.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
    }

    @Override
    public Integer handle(Message input) {
        validateInput(input);
        return storageEngine.fzDel(input.getKey());
    }

    @Override
    public CompletableFuture<Integer> handleAsync(Message input) {
        validateInput(input);
        return storageEngine.fzDelAsync(input.getKey());
    }
}
