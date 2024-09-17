import common.Config;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;

import static common.AdminClientFactory.adminClient;

public class ListTopics {
    public static void main(String[] args) throws Exception {
        try (final AdminClient client = adminClient()) {
            var options = new ListTopicsOptions();
            options.listInternal(false);
            var topics = client.listTopics(options).names().get();
            for (String topic : topics) {
                System.out.println(topic);
            }
        }
    }
}
