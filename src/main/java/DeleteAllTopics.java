import common.Config;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;

public class DeleteAllTopics {
    public static void main(String[] args) throws Exception {
        try (final AdminClient client = AdminClient.create(Config.get())) {
            var options = new ListTopicsOptions();
            options.listInternal(false);
            var topics = client.listTopics(options).names().get();
            client.deleteTopics(topics);
        }
    }
}
