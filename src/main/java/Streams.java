import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Streams {
    public static void main(String[] args) throws Exception {
        var builder = new StreamsBuilder();
        builder
                .stream("input", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> System.out.println(k + " : " + v))
                .to("output", Produced.with(Serdes.String(), Serdes.String()));

        var topology = builder.build();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9094");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "punctuator-test");
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
