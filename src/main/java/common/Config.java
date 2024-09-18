package common;

import java.util.UUID;

public class Config {
    public static final String SERVERS = "localhost:9094";

    public static String getAppId() {
        return System.getProperty("app", UUID.randomUUID().toString());
    }
}
