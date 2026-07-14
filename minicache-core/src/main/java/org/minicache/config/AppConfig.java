package org.minicache.config;

import java.io.InputStream;
import java.util.Properties;

public class AppConfig {
    public static Integer PORT;
    public static Integer STORAGE_SIZE;
    public static String VERSION;
    public static String STORAGE_TYPE;
    public static String NODE_ID;
    public static String CLUSTER_NODES;

    static {
        Properties prop = new Properties();
        try (InputStream input = AppConfig.class.getResourceAsStream("/config.properties")) {
            prop.load(input);
            STORAGE_SIZE = System.getenv("STORAGE_SIZE") != null
                    ? Integer.valueOf(System.getenv("STORAGE_SIZE"))
                    : Integer.valueOf(prop.getProperty("storage.size"));
            STORAGE_TYPE = System.getenv("STORAGE_TYPE") != null
                    ? System.getenv("STORAGE_TYPE")
                    : prop.getProperty("storage.type");
            PORT = System.getenv("CORE_PORT") != null
                    ? Integer.valueOf(System.getenv("CORE_PORT"))
                    : Integer.valueOf(prop.getProperty("server.port"));
            VERSION = System.getenv("VERSION") != null
                    ? System.getenv("VERSION")
                    : prop.getProperty("server.version");
            if (VERSION.equals(VERSIONS.V3)) {
                NODE_ID = System.getenv("NODE_ID");
                CLUSTER_NODES = System.getenv("CLUSTER_NODES");
            }
            if (STORAGE_TYPE == null) {
                STORAGE_TYPE = STORAGE_TYPES.SINGLE;
            }
        } catch (Exception ex) {
            STORAGE_SIZE = 500;
            PORT = 80;
        }
    }

    public static class VERSIONS {
        public static final String V1 = "v1";
        public static final String V2 = "v2";
        public static final String V3 = "v3";
    }

    public static class STORAGE_TYPES {
        public static final String SINGLE = "1";
        public static final String SEGMENT = "2";
        public static final String SHARED_NOTHING = "3";
    }
}
