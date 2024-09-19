import common.Config;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import processors.DelayProcessor;
import processors.SchedulerProcessor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Streams {
    public static void main(String[] args) throws Exception {

        StoreBuilder<TimestampedKeyValueStore<String, String>> delayStoreBuilder = Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore("delay-store"),
                Serdes.String(),
                Serdes.String());

        StoreBuilder<TimestampedKeyValueStore<String, String>> schedulerStoreBuilder = Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore("scheduler-store"),
                Serdes.String(),
                Serdes.String());

        var builder = new StreamsBuilder();


        builder.addStateStore(delayStoreBuilder);
        builder.addStateStore(schedulerStoreBuilder);

        builder
                .stream("scan1", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> System.out.println("s1 before " + k + " : " + v))
                .process(DelayProcessor::new, "delay-store")
                .peek((k, v) -> System.out.println("s1 after " + k + " : " + v))
                .to("scan2", Produced.with(Serdes.String(), Serdes.String()));

        builder
                .stream("input", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> System.out.println("s2 before " + k + " : " + v))
                .process(SchedulerProcessor::new, "scheduler-store")
                .peek((k, v) -> System.out.println("s2 after " + k + " : " + v))
                .to("output", Produced.with(Serdes.String(), Serdes.String()));

        var topology = builder.build();

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, Config.getAppId());
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

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
