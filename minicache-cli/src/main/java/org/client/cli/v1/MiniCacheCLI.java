package org.client.cli.v1;

import jdk.net.ExtendedSocketOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.client.cli.BaseMiniCacheCLI;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class MiniCacheCLI extends BaseMiniCacheCLI {
    private static final Logger log;

    static {
        log = LogManager.getLogger(MiniCacheCLI.class);
    }

    public static void execute(String host, Integer port, String commandText) {
        if (commandText == null || commandText.strip().isBlank()) return;

        // 1. Mở Socket kết nối tới Server cho LỆNH HIỆN TẠI
        try (Socket socket = new Socket(host, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            socket.setKeepAlive(true);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, 60);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, 10);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, 10);

            // 2. Mã hóa câu lệnh theo chuẩn giao thức Text-based MCP và gửi lên Server
            String encodedRequest = encodeMcpRequest(commandText);
            out.println(encodedRequest);

            // 3. Nhận phản hồi dạng Text từ Server
            String response = in.readLine();
            if (response == null) {
                log.error("(error) Unknown error");
                return;
            }

            // 4. Giải mã phản hồi và in kết quả ra màn hình CLI
            String decodedResponse = decodeMcpResponse(response);
            log.info(decodedResponse);

        } catch (Exception e) {
            log.error("(error) Cannot connect to server at {}:{}", host, port);
        }
    }

    private static String encodeMcpRequest(String userInput) {
        String[] tokens = userInput.strip().split("\\s+(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        StringBuilder mcpCmd = new StringBuilder();
        mcpCmd.append(tokens.length).append("|");

        for (String token : tokens) {
            mcpCmd.append(token.length()).append("|").append(token).append("|");
        }

        mcpCmd.setLength(mcpCmd.length() - 1);
        return mcpCmd.toString();
    }

    private static String decodeMcpResponse(String rawResponse) {
        if (rawResponse == null || rawResponse.isBlank()) {
            return "(nil)";
        }

        char type = rawResponse.charAt(0);
        switch (type) {
            case 'S':
                return rawResponse.substring(2);

            case 'I':
                return "(integer) " + rawResponse.substring(2);

            case 'N':
                return "(nil)";

            case 'E':
                return "(error) " + rawResponse.substring(2);

            case 'D':
                try {
                    int secondPipe = rawResponse.indexOf('|', 2);
                    if (secondPipe == -1) return rawResponse;

                    int length = Integer.parseInt(rawResponse.substring(2, secondPipe));
                    return "\"" + rawResponse.substring(secondPipe + 1, secondPipe + 1 + length) + "\"";
                } catch (Exception e) {
                    return "(error) ERR Protocol corruption";
                }

            default:
                return rawResponse;
        }
    }
}
