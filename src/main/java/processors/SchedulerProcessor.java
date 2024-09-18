package processors;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import java.time.Duration;

import static org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME;

public class SchedulerProcessor implements Processor<String, String, String, String> {

    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> processorContext) {
        this.context = processorContext;

        var punctuator = new MyPunctuator();
        this.context.schedule(Duration.ofSeconds(3), WALL_CLOCK_TIME, punctuator);
    }

    @Override
    public void process(Record<String, String> record) {
        System.out.println(record.value());
        context.forward(record);
        context.commit();
    }

    @Override
    public void close() {}

    private static class MyPunctuator implements Punctuator {

        @Override
        public void punctuate(long timestamp) {
            System.out.println(timestamp);
        }
    }

}