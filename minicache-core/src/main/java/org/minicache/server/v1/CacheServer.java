package org.minicache.server.v1;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.minicache.common.Command;
import org.minicache.common.Message;
import org.minicache.server.BaseCacheServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
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
                            try (clientSocket;
                                 BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                                 PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)) {
                                String inputLine;
                                while ((inputLine = reader.readLine()) != null) {
                                    var startTime = System.nanoTime();
                                    inputLine = inputLine.strip();
                                    if ("1|4|PING".equalsIgnoreCase(inputLine)) {
                                        writer.println("S|PONG");
                                        continue;
                                    }
                                    if ("1|4|EXIT".equalsIgnoreCase(inputLine)) {
                                        break;
                                    }

                                    var message = decode(inputLine);
                                    Command cmd = message.getCommand();

                                    if (cmd == null) {
                                        writer.println("E|ERR unknown command or wrong protocol format");
                                        continue;
                                    }

                                    var response = commandCacheHandler
                                            .get(cmd)
                                            .get()
                                            .handle(message);
                                    var endTime = System.nanoTime();
                                    log.info("DONE: {} ms", (endTime - startTime) * Math.pow(10, -6));
                                    writer.println(formatMcpResponse(cmd, response));
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
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

    private static String formatMcpResponse(Command cmd, Object result) {
        if (result == null) return "N|";

        return switch (cmd) {
            case PUT, BF_ADD, BF_INIT, Z_POS, Z_ADD, Z_RSCR, Z_RANGE, Z_SCR -> "S|" + result;
            case GET, LST_KEY -> {
                String value = (String) result;
                yield "D|" + value.length() + "|" + value;
            }
            case DELETE, EXISTS, CLEAR, BF_EXISTS, BF_RM, BF_RS, Z_INCR, Z_RANK, Z_DEL, Z_RM -> "I|" + result;
        };
    }

    private static Message decode(String rawInput) {
        Message msg = new Message();
        if (rawInput == null || rawInput.isBlank()) {
            return msg;
        }

        try {
            int firstPipe = rawInput.indexOf('|');
            if (firstPipe == -1)
                return msg;

            int numTokens = Integer.parseInt(rawInput.substring(0, firstPipe));
            java.util.List<String> tokens = new java.util.ArrayList<>();

            int currentIndex = firstPipe + 1;

            for (int i = 0; i < numTokens; i++) {
                int nextPipe = rawInput.indexOf('|', currentIndex);
                if (nextPipe == -1) return msg;

                int length = Integer.parseInt(rawInput.substring(currentIndex, nextPipe));

                String tokenData = rawInput.substring(nextPipe + 1, nextPipe + 1 + length);
                tokens.add(tokenData);

                currentIndex = nextPipe + 1 + length + 1;
            }

            return mapTokensToMessage(tokens);

        } catch (Exception e) {
            return msg;
        }
    }

    private static Message mapTokensToMessage(java.util.List<String> tokens) {
        Message msg = new Message();
        if (tokens.isEmpty()) return msg;

        String action = tokens.get(0).toUpperCase();
        switch (action) {
            case "SET":
                if (tokens.size() < 3) return msg;
                msg.setCommand(Command.PUT);
                msg.setKey(tokens.get(1));
                msg.setValue(tokens.get(2));
                if (tokens.size() >= 4) {
                    msg.setNotExists(Integer.parseInt(tokens.get(3)) == 1);
                }
                if (tokens.size() >= 5) {
                    msg.setTtl(Long.parseLong(tokens.get(4)));
                }
                break;
            case "GET":
                if (tokens.size() == 2) {
                    msg.setCommand(Command.GET);
                    msg.setKey(tokens.get(1));
                }
                break;
            case "DEL":
                if (tokens.size() == 2) {
                    msg.setCommand(Command.DELETE);
                    msg.setKey(tokens.get(1));
                }
                break;
            case "EXISTS":
                if (tokens.size() == 2) {
                    msg.setCommand(Command.EXISTS);
                    msg.setKey(tokens.get(1));
                }
                break;
            case "CLEAR":
                msg.setCommand(Command.CLEAR);
                break;
            case "KEYS":
                msg.setCommand(Command.LST_KEY);
                break;
            case "BF.INIT":
                if (tokens.size() < 4) return msg;
                msg.setCommand(Command.BF_INIT);
                msg.setKey(tokens.get(1));
                msg.setBloomFilterExpectedElements(Integer.valueOf(tokens.get(2)));
                msg.setBloomFilterFalsePositiveRate(Double.valueOf(tokens.get(3)));
                break;
            case "BF.ADD":
                if (tokens.size() == 3) {
                    msg.setCommand(Command.BF_ADD);
                    msg.setKey(tokens.get(1));
                    msg.setValue(tokens.get(2));
                }
                break;
            case "BF.EXISTS":
                if (tokens.size() >= 2) {
                    msg.setCommand(Command.BF_EXISTS);
                    msg.setKey(tokens.get(1));
                    if (tokens.size() == 3) {
                        msg.setValue(tokens.get(2));
                    }
                }
                break;
            case "BF.RM":
                if (tokens.size() == 2) {
                    msg.setCommand(Command.BF_RM);
                    msg.setKey(tokens.get(1));
                }
                break;
            case "BF.RS":
                if (tokens.size() == 2) {
                    msg.setCommand(Command.BF_RS);
                    msg.setKey(tokens.get(1));
                }
                break;
            case "Z.SCR":
                if (tokens.size() == 3) {
                    msg.setCommand(Command.Z_SCR);
                    msg.setKey(tokens.get(1));
                    msg.setZsMember(tokens.get(2));
                }
                break;
            case "Z.ADD":
                if (tokens.size() == 5) {
                    msg.setCommand(Command.Z_ADD);
                    msg.setKey(tokens.get(1));
                    msg.setZsScore(Double.valueOf(tokens.get(2)));
                    msg.setZsMember(tokens.get(3));
                    msg.setValue(tokens.get(4));
                }
                break;
            case "Z.RANK":
                if (tokens.size() == 3) {
                    msg.setCommand(Command.Z_RANK);
                    msg.setKey(tokens.get(1));
                    msg.setZsMember(tokens.get(2));
                }
                break;
            case "Z.RANGE":
                if (tokens.size() == 4) {
                    msg.setCommand(Command.Z_RANGE);
                    msg.setKey(tokens.get(1));
                    msg.setZsStartIdx(Integer.valueOf(tokens.get(2)));
                    msg.setZsStopIdx(Integer.valueOf(tokens.get(3)));
                }
                break;
            case "Z.RSCR":
                if (tokens.size() == 4) {
                    msg.setCommand(Command.Z_RSCR);
                    msg.setKey(tokens.get(1));
                    msg.setZsStartScr(Double.valueOf(tokens.get(2)));
                    msg.setZsStopScr(Double.valueOf(tokens.get(3)));
                }
                break;
            case "Z.POS":
                if (tokens.size() == 3) {
                    msg.setCommand(Command.Z_POS);
                    msg.setKey(tokens.get(1));
                    msg.setZsIdx(Integer.valueOf(tokens.get(2)));
                }
                break;
            case "Z.INCR":
                if (tokens.size() == 4) {
                    msg.setCommand(Command.Z_INCR);
                    msg.setKey(tokens.get(1));
                    msg.setZsMember(tokens.get(2));
                    msg.setZsScore(Double.valueOf(tokens.get(3)));
                }
                break;
            case "Z.RM":
                if (tokens.size() == 3) {
                    msg.setCommand(Command.Z_RM);
                    msg.setKey(tokens.get(1));
                    msg.setZsMember(tokens.get(2));
                }
                break;
            case "Z.DEL":
                if (tokens.size() == 2) {
                    msg.setCommand(Command.Z_DEL);
                    msg.setKey(tokens.get(1));
                }
                break;
        }
        return msg;
    }
}
