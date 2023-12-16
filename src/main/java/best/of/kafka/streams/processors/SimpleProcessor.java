package best.of.kafka.streams.processors;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

public class SimpleProcessor implements ProcessorSupplier<String, String, String, String> {
    @Override
    public Processor<String, String, String, String> get() {
        return new Processor<String, String, String, String>() {

            ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
               this.context = context;
            }

            @Override
            public void process(Record<String, String> record) {
                this.context.forward(new Record(record.key(), String.format("%s:%s", this.context.recordMetadata().get().topic(), record.value()), System.currentTimeMillis()));
            }

            @Override
            public void close() {

            }
        };
    }
}
