package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

import java.util.concurrent.CompletableFuture;

public class BfInitHandler extends BaseHandler implements ICacheHandler<String> {
    private static BfInitHandler handler;

    public static BfInitHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new BfInitHandler(storageEngine);
        }

        return handler;
    }

    private BfInitHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.BF_INIT.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getBloomFilterExpectedElements() == null) {
            throw new RuntimeException();
        }
        if (message.getBloomFilterFalsePositiveRate() == null) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return storageEngine.initBloomFilter(input.getKey(), input.getBloomFilterExpectedElements(),
                input.getBloomFilterFalsePositiveRate());
    }

    @Override
    public CompletableFuture<String> handleAsync(Message input) {
        validateInput(input);
        return storageEngine.initBloomFilterAsync(input.getKey(), input.getBloomFilterExpectedElements(),
                input.getBloomFilterFalsePositiveRate());
    }
}
