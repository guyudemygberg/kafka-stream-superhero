package best.of.kafka.streams.processors.joins;

import best.of.kafka.streams.processors.dto.exCart;
import best.of.kafka.streams.processors.dto.exCartTransaction;
import best.of.kafka.streams.processors.dto.reCheckout;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

@Component
public class global2 {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, exCartTransaction> cartTransactionStream = builder.stream("cartTransaction", Consumed.with(Serdes.String(), SerdesUtils.getSerde(exCartTransaction.class)));
        GlobalKTable<String, exCart> cartStream = builder.globalTable("cart22", Consumed.with(Serdes.String(), SerdesUtils.getSerde(exCart.class)));
        Produced<String, reCheckout> produced = Produced.with(Serdes.String(), SerdesUtils.getSerde(reCheckout.class));
        KeyValueMapper<String, exCartTransaction, String> keyValueMapper = (cartTransactionStreamKey, cartTransactionStreamValue) -> cartTransactionStreamValue.getCartId();
        ValueJoiner<exCartTransaction, exCart, reCheckout> valueJoiner = (cartTransaction, cart) -> new reCheckout(cartTransaction, cart);
        ValueJoinerWithKey<String, exCartTransaction, exCart, reCheckout> valueJoinerWithKey = (key, cartTransaction, cart) -> new reCheckout(key, cartTransaction, cart);

        KStream<String, reCheckout> innerJoin = cartTransactionStream.join(cartStream, keyValueMapper, valueJoiner);
        KStream<String, reCheckout> innerJoinWithKey = cartTransactionStream.join(cartStream, keyValueMapper, valueJoinerWithKey);
        KStream<String, reCheckout> innerJoinNamed = cartTransactionStream.join(cartStream, keyValueMapper, valueJoiner, Named.as("innerJoinNamed"));
        KStream<String, reCheckout> innerJoinWithKeyNamed = cartTransactionStream.join(cartStream, keyValueMapper, valueJoinerWithKey, Named.as("innerJoinWithKeyNamed"));
        KStream<String, reCheckout> leftJoin = cartTransactionStream.leftJoin(cartStream, keyValueMapper, valueJoiner);
        KStream<String, reCheckout> leftJoinWithKey = cartTransactionStream.leftJoin(cartStream, keyValueMapper, valueJoinerWithKey);
        KStream<String, reCheckout> leftJoinNamed = cartTransactionStream.leftJoin(cartStream, keyValueMapper, valueJoiner, Named.as("leftJoinNamed"));
        KStream<String, reCheckout> leftJoinWithKeyNamed = cartTransactionStream.leftJoin(cartStream, keyValueMapper, valueJoinerWithKey, Named.as("leftJoinWithKeyNamed"));

        innerJoin.to("innerJoin", produced);
        innerJoinWithKey.to("innerJoinWithKey", produced);
        innerJoinNamed.to("innerJoinNamed", produced);
        innerJoinWithKeyNamed.to("innerJoinWithKeyNamed", produced);
        leftJoin.to("leftJoin", produced);
        leftJoinWithKey.to("leftJoinWithKey", produced);
        leftJoinNamed.to("leftJoinNamed", produced);
        leftJoinWithKeyNamed.to("leftJoinWithKeyNamed", produced);

        return builder.build();
    }
}