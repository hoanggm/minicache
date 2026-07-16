package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

public class LstKeyHandler extends BaseHandler implements ICacheHandler<String> {
    private static LstKeyHandler handler;

    public static LstKeyHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new LstKeyHandler(storageEngine);
        }

        return handler;
    }

    private LstKeyHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.LST_KEY.equals(message.getCommand())) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return storageEngine.getAllKeys(input.getCommand());
    }
}
