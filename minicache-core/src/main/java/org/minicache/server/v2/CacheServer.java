package org.minicache.server.v2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.minicache.common.AsyncMDC;
import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.common.TraceContext;
import org.minicache.server.BaseCacheServer;
import org.minicache.util.CommonUtil;

import java.io.*;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.Executors;

public class CacheServer extends BaseCacheServer {
    private static final Logger log = LogManager.getLogger(CacheServer.class);

    public static void run(Integer port) {
        init(log, port);
        try {
            socketServer = new ServerSocket(Optional.ofNullable(port).orElse(80));
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                //noinspection InfiniteLoopStatement
                while (isRunning.get()) {
                    try {
                        var clientSocket = socketServer.accept();
                        executor.submit(() -> {
                            TraceContext.setTraceId(TraceContext.generateTraceId());
                            try (clientSocket;
                                 DataInputStream in = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream(), 2048));
                                 DataOutputStream out = new DataOutputStream(new BufferedOutputStream(clientSocket.getOutputStream(), 2048))) {
                                while (isRunning.get()) {
                                    // check protocol format with letter 'M'
                                    byte magic = in.readByte();
                                    if (magic != 0x4D) {
                                        sendBinaryResponse(out, (byte) 0xFF, "ERR Invalid format");
                                        continue;
                                    }

                                    // read binary from client
                                    byte opcode = in.readByte();
                                    short keyLength = in.readShort();
                                    int valueLength = in.readInt();

                                    byte[] keyBytes = new byte[keyLength];
                                    in.readFully(keyBytes);
                                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                                    byte[] valueBytes = new byte[valueLength];
                                    in.readFully(valueBytes);
                                    String value = new String(valueBytes, StandardCharsets.UTF_8);

                                    short notExists = in.readShort();
                                    int timeToLive = in.readInt();

                                    int bloomExpectedKeys = in.readInt();
                                    double bloomFalsePositiveRate = in.readDouble();

                                    double zsScore = in.readDouble();
                                    double zsStartScr = in.readDouble();
                                    double zsStopScr = in.readDouble();
                                    int zsIdx = in.readInt();
                                    int zsStartIdx = in.readInt();
                                    int zsStopIdx = in.readInt();
                                    String zsMember = in.readUTF();

                                    String geoMem = in.readUTF();
                                    double geoLat = in.readDouble();
                                    double geoLon = in.readDouble();
                                    String geoMem2 = in.readUTF();
                                    double geoRadius = in.readDouble();

                                    int limit = in.readInt();
                                    String hsField = in.readUTF();

                                    var startTime = System.nanoTime();
                                    if (opcode == 0x01) {
                                        sendBinaryResponse(out, (byte) 0x00, "PONG");
                                        continue;
                                    }
                                    // Command is EXIT
                                    if (opcode == 0x00) {
                                        break;
                                    }

                                    Command cmd = null;
                                    switch (opcode) {
                                        case 0x02 -> cmd = Command.GET;
                                        case 0x03 -> cmd = Command.PUT;
                                        case 0x04 -> cmd = Command.DELETE;
                                        case 0x05 -> cmd = Command.LST_KEY;
                                        case 0x06 -> cmd = Command.EXISTS;
                                        case 0x07 -> cmd = Command.CLEAR;
                                        case 0x08 -> cmd = Command.BF_INIT;
                                        case 0x09 -> cmd = Command.BF_ADD;
                                        case 0x10 -> cmd = Command.BF_EXISTS;
                                        case 0x11 -> cmd = Command.BF_RM;
                                        case 0x12 -> cmd = Command.BF_RS;
                                        case 0x21 -> cmd = Command.Z_SCR;
                                        case 0x13 -> cmd = Command.Z_ADD;
                                        case 0x14 -> cmd = Command.Z_POS;
                                        case 0x15 -> cmd = Command.Z_RANGE;
                                        case 0x16 -> cmd = Command.Z_RANK;
                                        case 0x17 -> cmd = Command.Z_INCR;
                                        case 0x18 -> cmd = Command.Z_RSCR;
                                        case 0x29 -> cmd = Command.Z_TOP;
                                        case 0x19 -> cmd = Command.Z_RM;
                                        case 0x20 -> cmd = Command.Z_DEL;
                                        case 0x22 -> cmd = Command.GEO_ADD;
                                        case 0x23 -> cmd = Command.GEO_SEARCH;
                                        case 0x24 -> cmd = Command.GEO_DIST;
                                        case 0x25 -> cmd = Command.GEO_DEL;
                                        case 0x26 -> cmd = Command.GEO_RM;
                                        case 0x27 -> cmd = Command.GEO_GET;
                                        case 0x28 -> cmd = Command.GEO_NB;
                                        case 0x30 -> cmd = Command.GEO_EXISTS;
                                        case 0x31 -> cmd = Command.GEO_ENCODE;
                                        case 0x32 -> cmd = Command.H_SET;
                                        case 0x33 -> cmd = Command.H_GET;
                                        case 0x34 -> cmd = Command.H_ALL;
                                        case 0x35 -> cmd = Command.H_RM;
                                        case 0x36 -> cmd = Command.H_DEL;
                                    }

                                    if (cmd == null || !commandCacheHandler.containsKey(cmd)) {
                                        sendBinaryResponse(out, (byte) 0xFF, "ERR Unknown command");
                                        continue;
                                    }

                                    var msg = new Message();
                                    msg.setCommand(cmd);
                                    msg.setKey(key);
                                    msg.setValue(value);
                                    msg.setNotExists(notExists == 1);
                                    msg.setTtl((long) timeToLive);
                                    msg.setBloomFilterExpectedElements(bloomExpectedKeys);
                                    msg.setBloomFilterFalsePositiveRate(bloomFalsePositiveRate);
                                    msg.setZsMember(zsMember);
                                    msg.setZsIdx(zsIdx);
                                    msg.setZsStartIdx(zsStartIdx);
                                    msg.setZsStopIdx(zsStopIdx);
                                    msg.setZsScore(zsScore);
                                    msg.setZsStartScr(zsStartScr);
                                    msg.setZsStopScr(zsStopScr);
                                    msg.setGeoMem(geoMem);
                                    msg.setGeoLat(geoLat);
                                    msg.setGeoLon(geoLon);
                                    msg.setGeoMem2(geoMem2);
                                    msg.setGeoRadius(geoRadius);
                                    msg.setLimit(limit);
                                    msg.setHsField(hsField);

                                    if (asyncResponse) {
                                        Command finalCmd = cmd;
                                        commandCacheHandler
                                                .get(cmd)
                                                .get()
                                                .handleAsync(msg)
                                                .thenAccept(AsyncMDC.wrap(response -> {
                                                    String resultStr = (response != null)
                                                            ? response.toString()
                                                            : "";

                                                    if (resultStr.isEmpty()) {
                                                        sendBinaryResponse(out, (byte) 0x02, null);
                                                    } else if (resultStr.startsWith("E|") || resultStr.startsWith("ERR")) {
                                                        sendBinaryResponse(out, (byte) 0xFF, resultStr);
                                                    } else {
                                                        if (finalCmd != Command.GET && CommonUtil.isInteger(resultStr)) {
                                                            sendBinaryResponse(out, (byte) 0x03, resultStr);
                                                        } else {
                                                            sendBinaryResponse(out, (byte) 0x01, resultStr);
                                                        }
                                                    }

                                                    var endTime = System.nanoTime();
                                                    log.info("DONE: {} ms", (endTime - startTime) * Math.pow(10, -6));
                                                }));
                                    } else {
                                        var response = commandCacheHandler
                                                .get(cmd)
                                                .get()
                                                .handle(msg);
                                        String resultStr = (response != null)
                                                ? response.toString()
                                                : "";

                                        if (resultStr.isEmpty()) {
                                            sendBinaryResponse(out, (byte) 0x02, null);
                                        } else if (resultStr.startsWith("E|") || resultStr.startsWith("ERR")) {
                                            sendBinaryResponse(out, (byte) 0xFF, resultStr);
                                        } else {
                                            if (cmd != Command.GET && CommonUtil.isInteger(resultStr)) {
                                                sendBinaryResponse(out, (byte) 0x03, resultStr);
                                            } else {
                                                sendBinaryResponse(out, (byte) 0x01, resultStr);
                                            }
                                        }

                                        var endTime = System.nanoTime();
                                        log.info("DONE: {} ms", (endTime - startTime) * Math.pow(10, -6));
                                    }
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            } finally {
                                TraceContext.clear();
                            }
                        });
                    } catch (Exception ex) {
                        if (!isRunning.get()) {
                            break;
                        }
                    }
                }
            } catch (Exception ex) {
                log.error(ex);
            }
        } catch (IOException ignored) {
        }
    }

    private static void sendBinaryResponse(DataOutputStream out, byte status, String data) {
        try {
            byte[] dataBytes = (data != null)
                    ? data.getBytes(StandardCharsets.UTF_8)
                    : new byte[0];

            out.writeByte(0x4D);
            // Status Byte (0x00=OK, 0x01=Data, 0x02=Null, 0xFF=Err, 0x03=Int)
            out.writeByte(status);
            out.writeInt(dataBytes.length);

            if (dataBytes.length > 0) {
                out.write(dataBytes);
            }
            out.flush();
        } catch (Exception ex) {
            log.error(ex);
        }
    }
}
