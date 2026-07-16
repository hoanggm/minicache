package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class GetHandler extends BaseHandler implements ICacheHandler<String> {
    private static GetHandler handler;

    public static GetHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new GetHandler(storageEngine);
        }

        return handler;
    }

    private GetHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.GET.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return storageEngine.get(input.getKey());
    }
}
