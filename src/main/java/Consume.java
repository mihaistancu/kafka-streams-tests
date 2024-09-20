import common.Config;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static java.lang.System.getProperty;
import static java.util.Arrays.asList;

public class Consume {
    public static void main(String[] args) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.SERVERS);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Config.getAppId());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, List.of(CooperativeStickyAssignor.class));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<String> topics = asList(getProperty("topics", "input").split(","));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            Thread mainThread = Thread.currentThread();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));

            consumer.subscribe(topics);

            long consumed = 0;
            long threshold = 0;

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                consumed += records.count();
                if (consumed > threshold) {
                    System.out.println("Consumed " + consumed);
                    threshold += 100000;
                }
            }
        }
        catch (WakeupException e) {
            System.out.println("Stopped");
        }
    }
}
