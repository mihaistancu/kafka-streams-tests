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

    private TimestampedKeyValueStore<String, String> store;
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

        try (KeyValueIterator<String, ValueAndTimestamp<String>> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, ValueAndTimestamp<String>> keyValue = iterator.next();
                var record = new Record<>(keyValue.key, keyValue.value.value(), keyValue.value.timestamp());
                context.forward(record);
                store.delete(keyValue.key);
            }
        }
    }

    @Override
    public void process(Record<String, String> record) {
        store.put(record.key(), ValueAndTimestamp.make(record.value(), context.currentSystemTimeMs()));
    }

    @Override
    public void close() {}
}