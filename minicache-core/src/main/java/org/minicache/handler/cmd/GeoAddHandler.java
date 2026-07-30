package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

import java.util.concurrent.CompletableFuture;

public class GeoAddHandler extends BaseHandler implements ICacheHandler<String> {
    private static GeoAddHandler handler;

    public static GeoAddHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new GeoAddHandler(storageEngine);
        }

        return handler;
    }

    private GeoAddHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.GEO_ADD.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getGeoMem() == null || message.getGeoMem().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getGeoLat() == null) {
            throw new RuntimeException();
        }
        if (message.getGeoLon() == null) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return storageEngine.geoAdd(input.getKey(), input.getGeoMem(), input.getGeoLat(), input.getGeoLon());
    }

    @Override
    public CompletableFuture<String> handleAsync(Message input) {
        validateInput(input);
        return storageEngine.geoAddAsync(input.getKey(), input.getGeoMem(), input.getGeoLat(), input.getGeoLon());
    }
}
