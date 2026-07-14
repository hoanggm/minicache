package org.minicache.server.v3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.common.TraceContext;
import org.minicache.config.AppConfig;
import org.minicache.consensus.raft.NodeState;
import org.minicache.consensus.raft.RaftListener;
import org.minicache.consensus.raft.RaftNode;
import org.minicache.server.BaseCacheServer;
import org.minicache.util.CommonUtil;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

public class CacheServer extends BaseCacheServer {
    private static final Logger log = LogManager.getLogger(CacheServer.class);
    private static RaftNode raftNode;

    public static void run(Integer port) {
        init(log, port);
        String nodeId = AppConfig.NODE_ID;
        String clusterNodesEnv = AppConfig.CLUSTER_NODES;

        // Init state machine
        RaftListener stateMachine = new RaftListener() {
            @Override
            public void onLogCommitted(String commandStr) {
                var startTime = System.nanoTime();
                if (commandStr == null || "NO_OP".equals(commandStr) || commandStr.isBlank()) {
                    return;
                }

                if (raftNode.getLeader().equals(raftNode.getNodeId())) {
                    return;
                }

                log.info("[STATE-MACHINE] Start applying replicated write command from LEADER");
                String[] tokens = commandStr.split("\\|", -1);
                if (tokens.length < 5) {
                    log.error("[STATE-MACHINE] Rejected corrupted command format: {}", commandStr);
                    return;
                }

                try {
                    var msg = new Message();
                    msg.setCommand(Command.valueOf(tokens[0]));
                    msg.setKey(tokens[1]);
                    msg.setValue(tokens[2]);
                    msg.setTtl(Long.parseLong(tokens[3]));
                    msg.setNotExists(Boolean.parseBoolean(tokens[4]));
                    msg.setBloomFilterExpectedElements(Integer.valueOf(tokens[5]));
                    msg.setBloomFilterFalsePositiveRate(Double.valueOf(tokens[6]));
                    msg.setZsScore(Double.valueOf(tokens[7]));
                    msg.setZsMember(tokens[8]);

                    commandCacheHandler.get(msg.getCommand())
                            .get()
                            .handle(msg);
                    var endTime = System.nanoTime();
                    log.info("[STATE-MACHINE] WRITE DONE: {} ms", (endTime - startTime) * Math.pow(10, -6));
                    log.info("[STATE-MACHINE] Successfully applied replicated write command to CacheEngine: <{}>", msg.getCommand());
                } catch (Exception e) {
                    log.error("[STATE-MACHINE] Error processing command: " + commandStr, e);
                }
            }

            @Override
            public void onBecomeLeader() {
                log.info("[CLUSTER-STATE] Node [{}] has won election [Term={}] and became LEADER",
                        nodeId, raftNode.getCurrentTerm());
            }

            @Override
            public void onBecomeFollower() {
                log.info("[CLUSTER-STATE] Node [{}] has transitioned to FOLLOWER", nodeId);
            }

            @Override
            public void onBecomeCandidate() {
                log.info("[CLUSTER-STATE] Node [{}] has transitioned to CANDIDATE", nodeId);
            }
        };

        // Start listen Client
        try {
            socketServer = new ServerSocket();
            socketServer.setReuseAddress(true);
            socketServer.setSoTimeout(30000);
            socketServer.bind(new InetSocketAddress(Optional.ofNullable(port).orElse(80)));

            // Start Raft network
            List<String> clusterNodes = Arrays.asList(clusterNodesEnv.split(","));
            raftNode = new RaftNode(nodeId, clusterNodes, stateMachine, log, true);
            raftNode.start();

            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {

                while (isRunning.get()) {
                    try {
                        Socket clientSocket = socketServer.accept();

                        executor.submit(() -> {
                            TraceContext.setTraceId(TraceContext.generateTraceId());

                            try (clientSocket;
                                 DataInputStream in = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream(), 2048));
                                 DataOutputStream out = new DataOutputStream(new BufferedOutputStream(clientSocket.getOutputStream(), 2048))) {

                                while (isRunning.get()) {
                                    // Kiểm tra cấu trúc giao thức bằng ký tự Magic 'M'
                                    byte magic = in.readByte();
                                    if (magic != 0x4D) {
                                        sendBinaryResponse(out, (byte) 0xFF, "ERR Invalid format");
                                        continue;
                                    }

                                    // Đọc các thông số nhị phân từ dòng I/O
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

                                    if (opcode == 0x01) {
                                        if (raftNode.getLeader().equals(raftNode.getNodeId())) {
                                            sendBinaryResponse(out, (byte) 0x00, "LEADER");
                                        } else {
                                            sendBinaryResponse(out, (byte) 0x00, "FOLLOWER");
                                        }
                                        continue;
                                    }
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
                                        case 0x19 -> cmd = Command.Z_RM;
                                        case 0x20 -> cmd = Command.Z_DEL;
                                    }

                                    if (cmd == null || !commandCacheHandler.containsKey(cmd)) {
                                        sendBinaryResponse(out, (byte) 0xFF, "ERR Unknown command");
                                        continue;
                                    }

                                    if (readCommands.contains(cmd)) {
                                        var startTime = System.nanoTime();

                                        var msg = new Message();
                                        msg.setCommand(cmd);
                                        msg.setKey(key);
                                        msg.setValue(value);
                                        msg.setZsMember(zsMember);
                                        msg.setZsIdx(zsIdx);
                                        msg.setZsStartIdx(zsStartIdx);
                                        msg.setZsStopIdx(zsStopIdx);
                                        msg.setZsScore(zsScore);
                                        msg.setZsStartScr(zsStartScr);
                                        msg.setZsStopScr(zsStopScr);

                                        var response = commandCacheHandler.get(cmd).get().handle(msg);
                                        String resultStr = (response != null) ? response.toString() : "";

                                        if (resultStr.isEmpty()) {
                                            sendBinaryResponse(out, (byte) 0x02, null);
                                        } else {
                                            if (cmd != Command.GET && CommonUtil.isInteger(resultStr)) {
                                                sendBinaryResponse(out, (byte) 0x03, resultStr);
                                            } else {
                                                sendBinaryResponse(out, (byte) 0x01, resultStr);
                                            }
                                        }

                                        var endTime = System.nanoTime();
                                        log.info("READ DONE: {} ms", (endTime - startTime) * Math.pow(10, -6));
                                    } else if (writeCommands.contains(cmd)) {
                                        // Quy tắc Raft: Nếu node nhận lệnh không phải Leader
                                        // -> Từ chối thẳng để bảo vệ tính nhất quán
                                        if (raftNode.getState() != NodeState.LEADER) {
                                            sendBinaryResponse(out, (byte) 0xFF, "ERR Not the Leader Node. Please forward write command to Leader.");
                                            continue;
                                        }

                                        var startTime = System.nanoTime();

                                        var msg = new Message();
                                        msg.setCommand(cmd);
                                        msg.setKey(key);
                                        msg.setValue(value);
                                        msg.setNotExists(notExists == 1);
                                        msg.setTtl((long) timeToLive);
                                        msg.setBloomFilterExpectedElements(bloomExpectedKeys);
                                        msg.setBloomFilterFalsePositiveRate(bloomFalsePositiveRate);
                                        msg.setValue(value);
                                        msg.setZsMember(zsMember);
                                        msg.setZsIdx(zsIdx);
                                        msg.setZsStartIdx(zsStartIdx);
                                        msg.setZsStopIdx(zsStopIdx);
                                        msg.setZsScore(zsScore);
                                        msg.setZsStartScr(zsStartScr);
                                        msg.setZsStopScr(zsStopScr);

                                        var response = commandCacheHandler
                                                .get(cmd)
                                                .get()
                                                .handle(msg);
                                        String resultStr = (response != null)
                                                ? response.toString()
                                                : "";
                                        if (resultStr.startsWith("E|") || resultStr.startsWith("ERR")) {
                                            sendBinaryResponse(out, (byte) 0xFF, resultStr);
                                        } else {
                                            if (CommonUtil.isInteger(resultStr)) {
                                                sendBinaryResponse(out, (byte) 0x03, resultStr);
                                            } else {
                                                sendBinaryResponse(out, (byte) 0x01, resultStr);
                                            }
                                        }

                                        // Định dạng dữ liệu thành chuỗi đặc tả Log duy nhất chuyển sang cho tầng mạng Raft
                                        String raftCommandStr = String.format("%s|%s|%s|%d|%b|%d|%f|%f|%s",
                                                cmd.name(), key, value,
                                                (long) timeToLive, notExists == 1,
                                                bloomExpectedKeys, bloomFalsePositiveRate,
                                                zsScore, zsMember);

                                        // Đồng bộ sang các node khác
                                        raftNode.propose(raftCommandStr);

                                        var endTime = System.nanoTime();
                                        log.info("WRITE DONE: {} ms", (endTime - startTime) * Math.pow(10, -6));
                                    }
                                }
                            } catch (IOException e) {
                                // log.warn("Client session closed or network disruption");
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

            out.writeByte(0x4D); // 'M'
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