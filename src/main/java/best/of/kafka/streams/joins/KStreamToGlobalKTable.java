package best.of.kafka.streams.joins;

import best.of.kafka.streams.dto.*;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class KStreamToGlobalKTable {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, CartTransaction> cartTransactionStream = builder.stream("cartTransaction", Consumed.with(Serdes.String(), SerdesUtils.getSerde(CartTransaction.class)));
        GlobalKTable<String, Cart> cartStream = builder.globalTable("cart", Consumed.with(Serdes.String(), SerdesUtils.getSerde(Cart.class)));
        Produced<String, Checkout> produced = Produced.with(Serdes.String(), SerdesUtils.getSerde(Checkout.class));
        KeyValueMapper<String, CartTransaction, String> keyValueMapper = (key, value) -> value.getCartId();

        ValueJoiner<CartTransaction, Cart, Checkout> valueJoiner = (cartTransaction, cart) -> new Checkout(cartTransaction, cart);
        ValueJoinerWithKey<String, CartTransaction, Cart, Checkout> valueJoinerWithKey = (key, cartTransaction, cart) -> new Checkout(key, cartTransaction, cart);

        KStream<String, Checkout> innerJoin = cartTransactionStream.join(cartStream, keyValueMapper, valueJoiner);
        KStream<String, Checkout> innerJoinWithKey = cartTransactionStream.join(cartStream, keyValueMapper, valueJoinerWithKey);
        KStream<String, Checkout> innerJoinNamed = cartTransactionStream.join(cartStream, keyValueMapper, valueJoiner, Named.as("innerJoinNamed"));
        KStream<String, Checkout> innerJoinWithKeyNamed = cartTransactionStream.join(cartStream, keyValueMapper, valueJoinerWithKey, Named.as("innerJoinWithKeyNamed"));

        KStream<String, Checkout> leftJoin = cartTransactionStream.leftJoin(cartStream, keyValueMapper, valueJoiner);
        KStream<String, Checkout> leftJoinWithKey = cartTransactionStream.leftJoin(cartStream, keyValueMapper, valueJoinerWithKey);
        KStream<String, Checkout> leftJoinNamed = cartTransactionStream.leftJoin(cartStream, keyValueMapper, valueJoiner, Named.as("leftJoinNamed"));
        KStream<String, Checkout> leftJoinWithKeyNamed = cartTransactionStream.leftJoin(cartStream, keyValueMapper, valueJoinerWithKey, Named.as("leftJoinWithKeyNamed"));

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
