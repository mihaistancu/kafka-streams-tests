package processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

import static org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME;

public class TestProcessor implements Processor<String, String, String, String> {

    private KeyValueStore<String, String> store;
    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> processorContext) {
        this.context = processorContext;

        this.store = processorContext.getStateStore("scheduler-store");
        this.context.schedule(Duration.ofSeconds(1), WALL_CLOCK_TIME, this::punctuate);
    }

    boolean firstStage = true;

    public void punctuate(long timestamp) {
        if (firstStage) {
            System.out.println("first stage " + Instant.now());
            for (int i=0;i<10_000_000;i++) {
                store.put(String.valueOf(i), String.valueOf(i));
            }
            firstStage = false;
        }
        else {
            System.out.print("second stage " + Instant.now());

            try (KeyValueIterator<String, String> iterator = store.all()) {
                int count;
                for (count=0; count<6000000 && iterator.hasNext(); count++) {
                    KeyValue<String, String> keyValue = iterator.next();
                    store.delete(keyValue.key);
                }

                System.out.println(count);
            }
        }
    }

    @Override
    public void process(Record<String, String> record) {

    }

    @Override
    public void close() {}
}