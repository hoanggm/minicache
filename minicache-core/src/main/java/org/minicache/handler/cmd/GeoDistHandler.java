package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

import java.util.concurrent.CompletableFuture;

public class GeoDistHandler extends BaseHandler implements ICacheHandler<String> {
    private static GeoDistHandler handler;

    public static GeoDistHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new GeoDistHandler(storageEngine);
        }

        return handler;
    }

    private GeoDistHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.GEO_DIST.equals(message.getCommand())) {
            throw new RuntimeException();
        }
        if (message.getKey() == null || message.getKey().isBlank()) {
            throw new RuntimeException();
        }
        if (message.getGeoMem() == null) {
            throw new RuntimeException();
        }
        if (message.getGeoMem2() == null) {
            throw new RuntimeException();
        }
    }

    @Override
    public String handle(Message input) {
        validateInput(input);
        return storageEngine.geoDist(input.getKey(), input.getGeoMem(), input.getGeoMem2());
    }

    @Override
    public CompletableFuture<String> handleAsync(Message input) {
        validateInput(input);
        return storageEngine.geoDistAsync(input.getKey(), input.getGeoMem(), input.getGeoMem2());
    }
}
