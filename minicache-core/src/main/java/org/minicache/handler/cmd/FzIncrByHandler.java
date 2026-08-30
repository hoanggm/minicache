package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

import java.util.concurrent.CompletableFuture;

public class FzIncrByHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static FzIncrByHandler handler;

    public static FzIncrByHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new FzIncrByHandler(storageEngine);
        }

        return handler;
    }

    private FzIncrByHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.FZ_INCR.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getValue() == null || message.getValue().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getFzFreq() == null) {
            throw new RuntimeException();
        }
    }

    @Override
    public Integer handle(Message input) {
        validateInput(input);
        return storageEngine.fzIncrBy(input.getKey(), input.getValue(), input.getFzFreq());
    }

    @Override
    public CompletableFuture<Integer> handleAsync(Message input) {
        validateInput(input);
        return storageEngine.fzIncrByAsync(input.getKey(), input.getValue(), input.getFzFreq());
    }
}
