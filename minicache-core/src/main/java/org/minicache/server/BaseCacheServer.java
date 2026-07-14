package org.minicache.server;

import org.apache.logging.log4j.Logger;
import org.minicache.common.Command;
import org.minicache.config.AppConfig;
import org.minicache.engine.CacheEngine;
import org.minicache.handler.ICacheHandler;
import org.minicache.handler.cmd.*;
import org.minicache.util.CommonUtil;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public abstract class BaseCacheServer {
    protected static final AtomicBoolean isRunning = new AtomicBoolean(true);
    protected static final HashMap<Command, Supplier<ICacheHandler<?>>> commandCacheHandler;
    protected static final CacheEngine cacheEngine;
    protected static ServerSocket socketServer;
    protected static List<Command> readCommands;
    protected static List<Command> writeCommands;

    static {
        if (AppConfig.STORAGE_TYPE.equals(AppConfig.STORAGE_TYPES.SEGMENT)) {
            cacheEngine = new org.minicache.engine.segment.CacheEngine(AppConfig.STORAGE_SIZE);
        } else if (AppConfig.STORAGE_TYPE.equals(AppConfig.STORAGE_TYPES.SHARED_NOTHING)) {
            cacheEngine = new org.minicache.engine.sharednothing.CacheEngine(AppConfig.STORAGE_SIZE);
        } else {
            cacheEngine = new org.minicache.engine.single.CacheEngine(AppConfig.STORAGE_SIZE);
        }

        commandCacheHandler = new HashMap<>();
        commandCacheHandler.put(Command.PUT, () -> PutHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.GET, () -> GetHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.DELETE, () -> DeleteHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.EXISTS, () -> ExistsHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.CLEAR, () -> ClearHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.LST_KEY, () -> LstKeyHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.BF_INIT, () -> BfInitHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.BF_ADD, () -> BfAddHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.BF_EXISTS, () -> BfExistsHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.BF_RM, () -> BfRemoveHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.BF_RS, () -> BfResetHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.Z_SCR, () -> ZScoreHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.Z_ADD, () -> ZAddHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.Z_RANK, () -> ZRankHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.Z_RM, () -> ZRemHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.Z_DEL, () -> ZDelHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.Z_INCR, () -> ZIncrHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.Z_POS, () -> ZPosHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.Z_RANGE, () -> ZRanHandler.getInstance(cacheEngine));
        commandCacheHandler.put(Command.Z_RSCR, () -> ZRanScoreHandler.getInstance(cacheEngine));

        readCommands = Arrays.asList(
                Command.GET,
                Command.BF_EXISTS,
                Command.EXISTS,
                Command.LST_KEY,
                Command.Z_SCR,
                Command.Z_POS,
                Command.Z_RSCR,
                Command.Z_RANK,
                Command.Z_RANGE
        );

        writeCommands = Arrays.asList(
                Command.PUT,
                Command.DELETE,
                Command.CLEAR,
                Command.BF_RS,
                Command.BF_RM,
                Command.BF_ADD,
                Command.BF_INIT,
                Command.Z_RM,
                Command.Z_DEL,
                Command.Z_INCR,
                Command.Z_ADD
        );
    }

    protected static void init(Logger log, Integer port) {
        log.info("Start MiniCache Server...");
        log.info("Listen port: {}", port);
        log.info("Date: {}", CommonUtil.formatDate(LocalDateTime.now(),
                "yyyy-MM-dd HH:mm:ss"));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Initiating graceful shutdown...");
            isRunning.set(false);
            if (socketServer != null && !socketServer.isClosed()) {
                try {
                    socketServer.close();
                    log.info("Closed successfully");
                } catch (IOException e) {
                    log.error("Error closing ServerSocket during shutdown", e);
                }
            }
        }));

        var cfgMap = cacheEngine.getInitCfg();
        if (cfgMap != null) {
            log.info("CacheEngine-Info: Number of Segments={}", cfgMap.get("segmentCount"));
            log.info("[Max-Size-Per-Segment={} bytes, Expected-Keys-Per-Segment={}]",
                    cfgMap.get("maxSizePerSegment"), cfgMap.get("segmentExpectedKeys"));
        }
    }
}
