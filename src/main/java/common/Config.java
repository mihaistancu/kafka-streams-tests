package common;

import java.util.HashMap;
import java.util.Map;

public class Config {
    public static Map<String, Object> get() {
        final Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9094");
        return config;
    }
}
