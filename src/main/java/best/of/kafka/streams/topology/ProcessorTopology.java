package best.of.kafka.streams.topology;

import best.of.kafka.streams.processors.SimpleProcessor;
import org.apache.kafka.streams.Topology;
import org.springframework.stereotype.Component;

@Component
public class ProcessorTopology {

//    @Bean
    public Topology createTopology(){
      Topology topology = new Topology();
      topology.addSource("Source", "disney-quotes", "basketball-quotes")
              .addProcessor("Processor", new SimpleProcessor(), "Source")
              .addSink("Sink", "processorTopic", "Processor");

      return topology;
    }
}
