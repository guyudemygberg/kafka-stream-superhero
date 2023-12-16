package best.of.kafka.streams.topology;

import best.of.kafka.streams.dto.MovieQuotes;
import best.of.kafka.streams.utils.KafkaSerdesUtils;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class KTableTopology {

    Gson gson = new Gson();
//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        Materialized materialized = Materialized.as("movies-quotes");
        Materialized materializedWithDefault = Materialized.as("movies-quotes-with-default");
        KTable simpleConsumerWithDefault = builder.table("star-wars-quotes");
        KTable materializedConsumerWithDefault = builder.table("disney-quotes", materializedWithDefault);
        KTable<String, MovieQuotes> simpleConsumer = builder.table("arnold-swerthenagger-quotes", Consumed.with(Serdes.String(), KafkaSerdesUtils.getSerdes(MovieQuotes.class, gson )));
        KTable materializedConsumer = builder.table("basketball-quotes", Consumed.with(Serdes.String(), Serdes.String()), materialized);

        simpleConsumerWithDefault.toStream().to("simpleConsumerWithDefault");
        materializedConsumerWithDefault.toStream().to("materializedConsumerWithDefault");
        simpleConsumer.toStream().to("simpleConsumer");
        materializedConsumer.toStream().to("materializedConsumer");
        return builder.build();
    }
}
