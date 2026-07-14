package org.client.cli.v2;

import jdk.net.ExtendedSocketOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.client.cli.BaseMiniCacheCLI;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class MiniCacheCLI extends BaseMiniCacheCLI {
    private static final Logger log;

    static {
        log = LogManager.getLogger(MiniCacheCLI.class);
    }

    public static void execute(String host, Integer port, String commandText) {
        if (commandText == null || commandText.strip().isBlank()) return;

        commandText = commandText.strip();
        String[] parts = commandText.split("\\s+(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        if (parts.length == 0 || parts[0].isBlank()) return;

        String action = parts[0].toUpperCase();

        try (Socket socket = new Socket(host, port);
             var out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 2048));
             var in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 2048))) {
            socket.setKeepAlive(true);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, 60);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, 10);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, 10);

            for (int i = 0; i < parts.length; i++) {
                if (parts[i].startsWith("\"") && parts[i].endsWith("\"") && parts[i].length() > 1) {
                    parts[i] = parts[i].substring(1, parts[i].length() - 1);
                }
            }

            switch (action) {
                case "PING" -> {
                    sendBinaryRequest(out, (byte) 0x01, null, null, "0", "0",
                            null, null, null,
                            null, null, null, null, null, null);
                }
                case "GET" -> {
                    if (parts.length < 2) {
                        log.info("(error) ERR Invalid command. Syntax: GET <key>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x02, parts[1], null, "0", "0",
                            null, null, null, null,
                            null, null, null, null, null);
                }
                case "EXISTS" -> {
                    if (parts.length < 2) {
                        log.info("(error) ERR Invalid command. Syntax: EXISTS <key>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x06, parts[1], null, "0", "0",
                            null, null, null, null,
                            null, null, null, null, null);
                }
                case "SET" -> {
                    if (parts.length < 3) {
                        log.info("(error) ERR Invalid command. Syntax: SET <key> <value> <not_exists: 0|1> <time_to_live>");
                        return;
                    }
                    String notExists = (parts.length > 3 && parts[3].equals("1")) ? parts[3] : "0";
                    String ttl = (parts.length > 4) ? parts[4] : "0";

                    sendBinaryRequest(out, (byte) 0x03, parts[1], parts[2], notExists, ttl,
                            null, null, null, null,
                            null, null, null, null, null);
                }
                case "DEL" -> {
                    if (parts.length < 2) {
                        log.info("(error) ERR Invalid command. Syntax: DEL <key>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x04, parts[1], null, "0", "0",
                            null, null, null, null,
                            null, null, null, null, null);
                }
                case "KEYS" -> {
                    sendBinaryRequest(out, (byte) 0x05, null, null, "0", "0",
                            null, null, null, null,
                            null, null, null, null, null);
                }
                case "CLEAR" -> {
                    sendBinaryRequest(out, (byte) 0x07, null, null, "0", "0",
                            null, null, null, null,
                            null, null, null, null, null);
                }
                case "EXIT" -> {
                    sendBinaryRequest(out, (byte) 0x00, null, null, "0", "0",
                            null, null, null, null,
                            null, null, null, null, null);
                }
                case "BF.INIT" -> {
                    if (parts.length < 4) {
                        log.info("(error) ERR Invalid command. Syntax: BF.INIT <key> <expected_keys_count> <false_positive_rate>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x08, parts[1], null, "0", "0",
                            parts[2], parts[3], null, null,
                            null, null, null, null, null);
                }
                case "BF.ADD" -> {
                    if (parts.length < 3) {
                        log.info("(error) ERR Invalid command. Syntax: BF.ADD <key> <value>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x09, parts[1], parts[2], "0", "0",
                            null, null, null, null,
                            null, null, null, null, null);
                }
                case "BF.EXISTS" -> {
                    if (parts.length < 2) {
                        log.info("(error) ERR Invalid command. Syntax: BF.EXISTS <key> <value>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x10, parts[1], (parts.length == 3) ? parts[2] : null, "0", "0",
                            null, null, null, null,
                            null, null, null, null, null);
                }
                case "BF.RM" -> {
                    if (parts.length < 2) {
                        log.info("(error) ERR Invalid command. Syntax: BF.RM <key>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x11, parts[1], null, "0", "0",
                            null, null, null, null,
                            null, null, null, null, null);
                }
                case "BF.RS" -> {
                    if (parts.length < 2) {
                        log.info("(error) ERR Invalid command. Syntax: BF.RS <key>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x12, parts[1], null, "0", "0",
                            null, null, null, null,
                            null, null, null, null, null);
                }
                case "Z.SCR" -> {
                    if (parts.length < 3) {
                        log.info("(error) ERR Invalid command. Syntax: Z.SCR <key> <member>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x21, parts[1], null, "0", "0",
                            null, null, parts[2], null,
                            null, null, null, null, null);
                }
                case "Z.ADD" -> {
                    if (parts.length < 5) {
                        log.info("(error) ERR Invalid command. Syntax: Z.ADD <key> <score> <member> <value>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x13, parts[1], parts[4], "0", "0",
                            null, null, parts[3], null,
                            null, null, parts[2], null, null);
                }
                case "Z.RANK" -> {
                    if (parts.length < 3) {
                        log.info("(error) ERR Invalid command. Syntax: Z.RANK <key> <member>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x16, parts[1], null, "0", "0",
                            null, null, parts[2], null,
                            null, null, null, null, null);
                }
                case "Z.RANGE" -> {
                    if (parts.length < 4) {
                        log.info("(error) ERR Invalid command. Syntax: Z.RANGE <key> <start> <stop>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x15, parts[1], null, "0", "0",
                            null, null, null, null,
                            parts[2], parts[3], null, null, null);
                }
                case "Z.RSCR" -> {
                    if (parts.length < 4) {
                        log.info("(error) ERR Invalid command. Syntax: Z.RSCR <key> <minScore> <maxScore>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x18, parts[1], null, "0", "0",
                            null, null, null, null,
                            null, null, null, parts[2], parts[3]);
                }
                case "Z.POS" -> {
                    if (parts.length < 3) {
                        log.info("(error) ERR Invalid command. Syntax: Z.POS <key> <position>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x14, parts[1], null, "0", "0",
                            null, null, null, parts[2],
                            null, null, null,null, null);
                }
                case "Z.INCR" -> {
                    if (parts.length < 4) {
                        log.info("(error) ERR Invalid command. Syntax: Z.INCR <key> <member> <increment>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x17, parts[1], null, "0", "0",
                            null, null, parts[2], null,
                            null, null, parts[3],null, null);
                }
                case "Z.RM" -> {
                    if (parts.length < 3) {
                        log.info("(error) ERR Invalid command. Syntax: Z.RM <key> <member>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x19, parts[1], null, "0", "0",
                            null, null, parts[2], null,
                            null, null, null,null, null);
                }
                case "Z.DEL" -> {
                    if (parts.length < 2) {
                        log.info("(error) ERR Invalid command. Syntax: Z.DEL <key>");
                        return;
                    }
                    sendBinaryRequest(out, (byte) 0x20, parts[1], null, "0", "0",
                            null, null, null, null,
                            null, null, null,null, null);
                }
                default -> {
                    log.info("(error) ERR Invalid command: '{}'", action);
                    return;
                }
            }

            handleServerResponse(in);

        } catch (Exception e) {
            log.error("(error) Cannot connect to server at {}:{}", host, port);
        }
    }

    private static void handleServerResponse(DataInputStream in) {
        try {
            byte magic = in.readByte();
            if (magic != 0x4D) {
                log.info("(error) ERR Protocol corruption");
            }

            byte status = in.readByte();
            int dataLength = in.readInt();

            byte[] dataBytes = new byte[dataLength];
            if (dataLength > 0) {
                in.readFully(dataBytes);
            }
            String resultString = new String(dataBytes, StandardCharsets.UTF_8);

            switch (status) {
                case 0x00 -> {
                    switch (resultString) {
                        case "PONG" -> log.info("PONG");
                        case "LEADER" -> log.info("LEADER");
                        case "FOLLOWER" -> log.info("FOLLOWER");
                        default -> log.info("OK");
                    }
                }

                case 0x01 -> log.info("\"" + resultString + "\"");

                case 0x03 -> log.info("(integer) " + resultString);

                case 0x02 -> log.info("(nil)");

                case (byte) 0xFF -> log.info("(error) " + resultString);

                default -> log.info("(error) Unknown server status code: " + status);
            }
        } catch (EOFException ex) {
            log.info("EXIT");
        } catch (Exception ex) {
            log.error(ex);
        }
    }

    private static void sendBinaryRequest(DataOutputStream out, byte code, String key, String value,
                                          String notExists, String timeToLive, String bloomFilterExpectedElements,
                                          String bloomFilterFalsePositiveRate, String zsMember,
                                          String zsIdx, String zsStartIdx, String zsStopIdx,
                                          String zsScore, String zsStartScr, String zsStopScr) {
        try {
            var keyBytes = (key != null)
                    ? key.getBytes(StandardCharsets.UTF_8)
                    : new byte[0];
            var valueBytes = (value != null)
                    ? value.getBytes(StandardCharsets.UTF_8)
                    : new byte[0];

            // write header
            // letter 'M' -> mark as start point read bytes from here
            out.writeByte(0x4D);
            out.writeByte(code);
            out.writeShort(keyBytes.length);
            out.writeInt(valueBytes.length);

            // write data
            if (keyBytes.length > 0) {
                out.write(keyBytes);
            }
            if (valueBytes.length > 0) {
                out.write(valueBytes);
            }
            short sNotExists = 0;
            if (notExists != null && !notExists.isBlank()) {
                sNotExists = Short.parseShort(notExists);
            }
            out.writeShort(sNotExists);
            int iTtl = 0;
            if (timeToLive != null && !timeToLive.isBlank()) {
                iTtl = Integer.parseInt(timeToLive);
            }
            out.writeInt(iTtl);

            // for bloom-filter
            out.writeInt(bloomFilterExpectedElements != null
                    ? Integer.parseInt(bloomFilterExpectedElements)
                    : 0);
            out.writeDouble(bloomFilterFalsePositiveRate != null
                    ? Double.parseDouble(bloomFilterFalsePositiveRate)
                    : 0d);

            // for skip-list
            out.writeDouble(zsScore != null ? Double.parseDouble(zsScore) : 0d);
            out.writeDouble(zsStartScr != null ? Double.parseDouble(zsStartScr) : 0d);
            out.writeDouble(zsStopScr != null ? Double.parseDouble(zsStopScr) : 0d);
            out.writeInt(zsIdx != null ? Integer.parseInt(zsIdx) : 0);
            out.writeInt(zsStartIdx != null ? Integer.parseInt(zsStartIdx) : 0);
            out.writeInt(zsStopIdx != null ? Integer.parseInt(zsStopIdx) : 0);
            out.writeUTF(zsMember != null ? zsMember : "");

            out.flush();
        } catch (Exception ex) {
            log.error(ex);
        }
    }
}
