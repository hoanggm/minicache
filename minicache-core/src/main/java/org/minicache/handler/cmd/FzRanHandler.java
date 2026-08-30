package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

import java.util.concurrent.CompletableFuture;

public class FzRanHandler extends BaseHandler implements ICacheHandler<String> {
    private static FzRanHandler handler;

    public static FzRanHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new FzRanHandler(storageEngine);
        }

        return handler;
    }

    private FzRanHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.FZ_RANDOM.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getLimit() == null) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return storageEngine.fzRan(input.getKey(), input.getLimit());
    }

    @Override
    public CompletableFuture<String> handleAsync(Message input) {
        validateInput(input);
        return storageEngine.fzRanAsync(input.getKey(), input.getLimit());
    }
}
