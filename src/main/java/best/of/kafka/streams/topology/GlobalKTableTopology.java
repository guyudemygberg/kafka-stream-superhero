package best.of.kafka.streams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.cglib.core.Transformer;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class GlobalKTableTopology {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        Materialized materialized = Materialized.as("movies-quotes");
        Materialized materializedWithDefault = Materialized.as("movies-quotes-with-default");
        GlobalKTable simpleConsumerWithDefault = builder.globalTable("star-wars-quotes");
        GlobalKTable materializedConsumerWithDefault = builder.globalTable("disney-quotes", materializedWithDefault);
        GlobalKTable simpleConsumer = builder.globalTable("arnold-swerthenagger-quotes", Consumed.with(Serdes.String(), Serdes.String()));
        GlobalKTable materializedConsumer = builder.globalTable("basketball-quotes", Consumed.with(Serdes.String(), Serdes.String()), materialized);

//        simpleConsumerWithDefault.toStream().to("simpleConsumerWithDefault");
//        materializedConsumerWithDefault.toStream().to("materializedConsumerWithDefault");
//        simpleConsumer.toStream().to("simpleConsumer");
//        materializedConsumer.toStream().to("materializedConsumer");
        return builder.build();
    }
}
