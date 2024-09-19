package processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Duration;

import static org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME;

public class DelayProcessor implements Processor<String, String, String, String> {

    private TimestampedKeyValueStore<String, String> store;
    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> processorContext) {
        this.context = processorContext;
        this.store = processorContext.getStateStore("delay-store");
        this.context.schedule(Duration.ofSeconds(3), WALL_CLOCK_TIME, this::punctuate);
    }

    public void punctuate(long timestamp) {
        System.out.println(timestamp);

        try (KeyValueIterator<String, ValueAndTimestamp<String>> iterator = store.all()) {
            if (iterator.hasNext()) {

                KeyValue<String, ValueAndTimestamp<String>> keyValue = iterator.next();
                long duration = timestamp - keyValue.value.timestamp();
                if (duration > 20000) {
                    while (true) {
                        var record = new Record<>(keyValue.key, keyValue.value.value(), keyValue.value.timestamp());
                        context.forward(record);
                        store.delete(keyValue.key);

                        if (iterator.hasNext()) {
                            keyValue = iterator.next();
                        }
                        else {
                            return;
                        }
                    }
                }
            }
        }
    }

    @Override
    public void process(Record<String, String> record) {
        store.put(record.key(), ValueAndTimestamp.make(record.value(), record.timestamp()));
    }

    @Override
    public void close() {}
}