import common.Config;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import processors.SchedulerProcessor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import processors.TestProcessor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class Streams {
    public static void main(String[] args) throws Exception {

        StoreBuilder<KeyValueStore<String, String>> schedulerStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("scheduler-store"),
                Serdes.String(),
                Serdes.String());

        var builder = new StreamsBuilder();

        builder.addStateStore(schedulerStoreBuilder);

        AtomicLong before = new AtomicLong();
        AtomicLong after = new AtomicLong();

        builder
                .stream("input", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> {
                    before.getAndIncrement();
                    if (before.get() % 100000 == 0) {
                        System.out.println("before " + before.get());
                    }
                })
                //.process(TestProcessor::new, "scheduler-store")
                .process(SchedulerProcessor::new, "scheduler-store")
                .peek((k, v) -> {
                    after.getAndIncrement();
                    if (after.get() % 100000 == 0) {
                        System.out.println("after " + after.get());
                    }
                })
                .to("output", Produced.with(Serdes.String(), Serdes.String()));

        var topology = builder.build();

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, Config.getAppId());
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 900000);

        var latch = new CountDownLatch(1);

        try (var streams = new KafkaStreams(topology, properties)) {

            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    streams.close(Duration.ofSeconds(5));
                    latch.countDown();
                }
            });

            streams.start();
            latch.await();
        }
    }
}
