package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

import java.util.concurrent.CompletableFuture;

public class GeoEncodeHandler extends BaseHandler implements ICacheHandler<String> {
    private static GeoEncodeHandler handler;

    public static GeoEncodeHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new GeoEncodeHandler(storageEngine);
        }

        return handler;
    }

    private GeoEncodeHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.GEO_ENCODE.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getGeoMem() == null || message.getGeoMem().isBlank()) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return storageEngine.geoEncode(input.getKey(), input.getGeoMem());
    }

    @Override
    public CompletableFuture<String> handleAsync(Message input) {
        validateInput(input);
        return storageEngine.geoEncodeAsync(input.getKey(), input.getGeoMem());
    }
}
