package common;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

public class AdminClientFactory {
    public static AdminClient adminClient() {
        Map<String, Object> config = new HashMap<>();
        config.put(BOOTSTRAP_SERVERS_CONFIG, Config.SERVERS);
        return AdminClient.create(config);
    }
}
