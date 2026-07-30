package org.minicache.handler.cmd;

import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.engine.StorageEngine;
import org.minicache.handler.BaseHandler;
import org.minicache.handler.ICacheHandler;

import java.util.concurrent.CompletableFuture;

public class GeoRmHandler extends BaseHandler implements ICacheHandler<Integer> {
    private static GeoRmHandler handler;

    public static GeoRmHandler getInstance(StorageEngine storageEngine) {
        if (handler == null) {
            handler = new GeoRmHandler(storageEngine);
        }

        return handler;
    }

    private GeoRmHandler(StorageEngine storageEngine) {
        super(storageEngine);
    }

    @Override
    public void validateInput(Message message) {
        if (message == null) {
            throw new RuntimeException();
        }
        if (!Command.GEO_RM.equals(message.getCommand())) {
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
        return storageEngine.geoRem(input.getKey(), input.getGeoMem());
    }

    @Override
    public CompletableFuture<Integer> handleAsync(Message input) {
        validateInput(input);
        return storageEngine.geoRemAsync(input.getKey(), input.getGeoMem());
    }
}
