package org.client.config;

import java.io.InputStream;
import java.util.Properties;

public class AppConfig {
    public static String DF_HOST;
    public static Integer DF_PORT;
    public static String VERSION;

    static {
        Properties prop = new Properties();
        try (InputStream input = AppConfig.class.getResourceAsStream("/config.properties")) {
            prop.load(input);
            DF_HOST = System.getenv("CORE_HOST") != null
                    ? System.getenv("CORE_HOST")
                    : prop.getProperty("server.host");
            DF_PORT = System.getenv("CORE_PORT") != null
                    ? Integer.parseInt(System.getenv("CORE_PORT"))
                    : Integer.parseInt(prop.getProperty("server.port"));
            VERSION = System.getenv("VERSION") != null
                    ? System.getenv("VERSION")
                    : prop.getProperty("client.version");
        } catch (Exception ex) {
            DF_HOST = "127.0.0.1";
            DF_PORT = 80;
        }
    }

    public static class VERSIONS {
        public static final String V1 = "v1";
        public static final String V2 = "v2";
    }
}
