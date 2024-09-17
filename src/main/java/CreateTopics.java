import common.Config;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.List;

public class CreateTopics {
    public static void main(String[] args) {
        try (final AdminClient client = AdminClient.create(Config.get())) {

            final List<NewTopic> topics = new ArrayList<>();

            int partitions = 1;
            short replication = 1;
            topics.add(new NewTopic("input", partitions, replication));
            topics.add(new NewTopic("output", partitions, replication));

            client.createTopics(topics);
        }
    }
}
