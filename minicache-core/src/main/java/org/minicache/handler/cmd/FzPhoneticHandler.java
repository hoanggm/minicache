package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

import java.util.concurrent.CompletableFuture;

public class FzPhoneticHandler extends BaseHandler implements ICacheHandler<String> {
    private static FzPhoneticHandler handler;

    public static FzPhoneticHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new FzPhoneticHandler(storageEngine);
        }

        return handler;
    }

    private FzPhoneticHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.FZ_PHONETIC.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getValue() == null || message.getValue().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getLimit() == null) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return storageEngine.fzPhonetic(input.getKey(), input.getValue(), input.getLimit());
    }

    @Override
    public CompletableFuture<String> handleAsync(Message input) {
        validateInput(input);
        return storageEngine.fzPhoneticAsync(input.getKey(), input.getValue(), input.getLimit());
    }
}
