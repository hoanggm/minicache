package org.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.client.config.AppConfig;

import java.util.Optional;
import java.util.Scanner;

public class Main {
    private static final Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            String host = AppConfig.DF_HOST;
            int port = AppConfig.DF_PORT;
            try {
                log.info("TYPE HOST (Default '" + host + "'): ");
                String inputHost = scanner.nextLine().trim();
                if (!inputHost.isEmpty()) {
                    host = inputHost;
                }

                log.info("TYPE PORT (Default '" + port + "'): ");
                String inputPort = scanner.nextLine().trim();
                if (!inputPort.isEmpty()) {
                    port = Integer.parseInt(inputPort);
                }
            } catch (NumberFormatException e) {
                log.error("(error) Invalid port !!!");
                continue;
            }

            String version = Optional.ofNullable(AppConfig.VERSION)
                    .orElse(AppConfig.VERSIONS.V2);

            boolean keepSession = true;
            boolean check = true;

            while (keepSession) {
                if (check) {
                    log.info("Connected <---> {}:{}", host, port);
                    log.info("MiniCache-CLI: *Start session");
                    check = false;
                }

                if (!scanner.hasNextLine()) {
                    keepSession = false;
                    check = true;
                    log.info("MiniCache-CLI: *End session");
                    continue;
                }

                String commandText = scanner.nextLine().trim();
                if (commandText.isEmpty()) {
                    continue;
                }

                try {
                    switch (version) {
                        case AppConfig.VERSIONS.V1 -> org.client.cli.v1.MiniCacheCLI.execute(host, port, commandText);
                        case AppConfig.VERSIONS.V2 -> org.client.cli.v2.MiniCacheCLI.execute(host, port, commandText);
                    }
                } catch (Exception e) {
                    log.error("(error) Error when execute command: {}", e.getMessage());
                }

                if ("exit".equalsIgnoreCase(commandText)) {
                    keepSession = false;
                    check = true;
                    log.info("MiniCache-CLI: *End session");
                }
            }
        }
    }
}