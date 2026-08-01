package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

import java.util.concurrent.CompletableFuture;

public class GeoExistsHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static GeoExistsHandler handler;

    public static GeoExistsHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new GeoExistsHandler(storageEngine);
        }

        return handler;
    }

    private GeoExistsHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.GEO_EXISTS.equals(message.getCommand())) {
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
    public Integer handle(Message input) {
        validateInput(input);
        return storageEngine.geoExists(input.getKey(), input.getGeoMem());
    }

    @Override
    public CompletableFuture<Integer> handleAsync(Message input) {
        validateInput(input);
        return storageEngine.geoExistsAsync(input.getKey(), input.getGeoMem());
    }
}
