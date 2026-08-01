package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

import java.util.concurrent.CompletableFuture;

public class ZTopHandler extends BaseHandler implements ICacheHandler<String> {
    private static ZTopHandler handler;

    public static ZTopHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new ZTopHandler(storageEngine);
        }

        return handler;
    }

    private ZTopHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.Z_TOP.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getZsStartIdx() == null) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return storageEngine.zTop(input.getKey(), input.getZsStartIdx());
    }

    @Override
    public CompletableFuture<String> handleAsync(Message input) {
        validateInput(input);
        return storageEngine.zTopAsync(input.getKey(), input.getZsStartIdx());
    }
}
