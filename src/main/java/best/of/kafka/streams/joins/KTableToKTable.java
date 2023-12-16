package best.of.kafka.streams.joins;

import best.of.kafka.streams.dto.Coupon;
import best.of.kafka.streams.dto.JoinedClass;
import best.of.kafka.streams.dto.Product;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class KTableToKTable {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Product> productStream = builder.table("product", Consumed.with(Serdes.String(), SerdesUtils.getSerde(Product.class)));
        KTable<String, Coupon> couponStream = builder.table("coupon", Consumed.with(Serdes.String(), SerdesUtils.getSerde(Coupon.class)));
        Produced<String, JoinedClass> produced = Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class));
        ValueJoiner<Product, Coupon, JoinedClass> valueJoiner = (product, coupon) -> new JoinedClass(product, coupon);
        Materialized<String, JoinedClass, KeyValueStore<Bytes, byte[]>> materialized = Materialized.as("KTableToKTableView").with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class));

        KTable<String, JoinedClass> innerJoin = productStream.join(couponStream, valueJoiner);
        KTable<String, JoinedClass> innerJoinNamed = productStream.join(couponStream, valueJoiner, Named.as("innerJoinNamed"));
        KTable<String, JoinedClass> innerJoinView = productStream.join(couponStream, valueJoiner, materialized);
        KTable<String, JoinedClass> innerJoinNamedView = productStream.join(couponStream, valueJoiner, Named.as("innerJoinNamedView"), materialized);

        KTable<String, JoinedClass> leftJoin = productStream.leftJoin(couponStream, valueJoiner);
        KTable<String, JoinedClass> leftJoinNamed = productStream.leftJoin(couponStream, valueJoiner, Named.as("leftJoinNamed"));
        KTable<String, JoinedClass> leftJoinView = productStream.leftJoin(couponStream, valueJoiner, materialized);
        KTable<String, JoinedClass> leftJoinNamedView = productStream.leftJoin(couponStream, valueJoiner, Named.as("leftJoinNamedView"), materialized);

        KTable<String, JoinedClass> outerJoin = productStream.outerJoin(couponStream, valueJoiner);
        KTable<String, JoinedClass> outerJoinNamed = productStream.outerJoin(couponStream, valueJoiner, Named.as("outerJoinNamed"));
        KTable<String, JoinedClass> outerJoinView = productStream.outerJoin(couponStream, valueJoiner, materialized);
        KTable<String, JoinedClass> outerJoinNamedView = productStream.outerJoin(couponStream, valueJoiner, Named.as("outerJoinNamedView"), materialized);

        innerJoin.toStream().to("innerJoin", produced);
        innerJoinNamed.toStream().to("innerJoinNamed", produced);
        innerJoinView.toStream().to("innerJoinView", produced);
        innerJoinNamedView.toStream().to("innerJoinNamedView", produced);
        leftJoin.toStream().to("leftJoin", produced);
        leftJoinNamed.toStream().to("leftJoinNamed", produced);
        leftJoinView.toStream().to("leftJoinView", produced);
        leftJoinNamedView.toStream().to("leftJoinNamedView", produced);
        outerJoin.toStream().to("outerJoin", produced);
        outerJoinNamed.toStream().to("outerJoinNamed", produced);
        outerJoinView.toStream().to("outerJoinView", produced);
        outerJoinNamedView.toStream().to("outerJoinNamedView", produced);

        return builder.build();
    }
}
