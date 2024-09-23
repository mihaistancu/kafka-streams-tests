package processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.*;

import java.time.Duration;
import java.time.Instant;

import static org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME;

public class SchedulerProcessor implements Processor<String, String, String, String> {

    private KeyValueStore<String, String> store;
    private ProcessorContext<String, String> context;

    private long start = 0;

    @Override
    public void init(ProcessorContext<String, String> processorContext) {
        this.context = processorContext;
        this.start = Instant.now().toEpochMilli();

        this.store = processorContext.getStateStore("scheduler-store");
        this.context.schedule(Duration.ofSeconds(1), WALL_CLOCK_TIME, this::punctuate);
    }

    public void punctuate(long timestamp) {
        System.out.println(timestamp + " " + Instant.now());

        if (timestamp < start + 20000) {
            return;
        }

        long count = 0;
        try (KeyValueIterator<String, String> iterator = store.all()) {
            while (iterator.hasNext()) {
                count++;
                KeyValue<String, String> keyValue = iterator.next();
                var record = new Record<>(keyValue.key, keyValue.value, timestamp);
                context.forward(record);
                //store.delete(keyValue.key);
            }

            System.out.println("punctuate processed: " + count);
        }
    }

    @Override
    public void process(Record<String, String> record) {
        store.put(record.key(), record.value());
    }

    @Override
    public void close() {}
}