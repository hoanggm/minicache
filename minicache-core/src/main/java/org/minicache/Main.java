package org.minicache;

import org.minicache.config.AppConfig;

import java.util.Optional;

public class Main {
    public static void main(String[] args) {
        String version = Optional.ofNullable(AppConfig.VERSION)
                .orElse(AppConfig.VERSIONS.V2);
        switch (version) {
            case AppConfig.VERSIONS.V1 -> org.minicache.server.v1.CacheServer.run(AppConfig.PORT);
            case AppConfig.VERSIONS.V2 -> org.minicache.server.v2.CacheServer.run(AppConfig.PORT);
            case AppConfig.VERSIONS.V3 -> org.minicache.server.v3.CacheServer.run(AppConfig.PORT);
        }
    }
}