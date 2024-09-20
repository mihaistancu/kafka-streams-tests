import common.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Optional;
import java.util.Properties;

public class Produce {
    public static void main(String[] args) throws Exception {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.SERVERS);
        //props.put(ProducerConfig.ACKS_CONFIG, "all");
        //props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        String topic = System.getProperty("topic", "input");
        Optional<String> key = Optional.ofNullable(System.getProperty("key"));
        int start = Integer.parseInt(System.getProperty("start", "0"));
        int count = Integer.parseInt(System.getProperty("count", "10"));

        try (KafkaProducer<String,String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String value = topic + (i + start);
                var message = new ProducerRecord<>(topic, key.orElse(value), value);
                producer.send(message).get();
            }
        }
    }
}
