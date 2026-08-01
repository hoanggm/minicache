package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

import java.util.concurrent.CompletableFuture;

public class GeoNbHandler extends BaseHandler implements ICacheHandler<String> {
    private static GeoNbHandler handler;

    public static GeoNbHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new GeoNbHandler(storageEngine);
        }

        return handler;
    }

    private GeoNbHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.GEO_NB.equals(message.getCommand())) {
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
        return storageEngine.geoNb(input.getKey(), input.getGeoMem());
    }

    @Override
    public CompletableFuture<String> handleAsync(Message input) {
        validateInput(input);
        return storageEngine.geoNbAsync(input.getKey(), input.getGeoMem());
    }
}
