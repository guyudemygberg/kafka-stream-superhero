package best.of.kafka.streams.joins;

import best.of.kafka.streams.dto.Coupon;
import best.of.kafka.streams.dto.JoinedClass;
import best.of.kafka.streams.dto.Product;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class KStreamToKTable {

    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Product> productStream = builder.stream("product", Consumed.with(Serdes.String(), SerdesUtils.getSerde(Product.class)));
        KTable<String, Coupon> couponStream = builder.table("coupon", Consumed.with(Serdes.String(), SerdesUtils.getSerde(Coupon.class)));
        KStream<String, String> productStreamString = productStream.mapValues((product) -> product.getName());
        KTable<String, String> couponStreamString = couponStream.mapValues((coupon) -> coupon.getDiscount()+"");

        Joined joined = Joined.with(Serdes.String(), SerdesUtils.getSerde(Product.class), SerdesUtils.getSerde(Coupon.class));
        Produced<String, JoinedClass> produced = Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class));

        ValueJoiner<Product, Coupon, JoinedClass> valueJoiner = (product, coupon) -> new JoinedClass(product, coupon);
        ValueJoinerWithKey<String, Product, Coupon, JoinedClass> valueJoinerWithKey = (key, product, coupon) -> new JoinedClass(key, product, coupon);
        ValueJoiner<String, String, JoinedClass> valueJoinerString = (name, discount) -> new JoinedClass(name, discount);
        ValueJoinerWithKey<String, String, String, JoinedClass> valueJoinerWithKeyString = (key, name, discount) -> new JoinedClass(key, name, discount);

        KStream<String, JoinedClass> innerJoin = productStream.join(couponStream, valueJoiner, joined);
        KStream<String, JoinedClass> innerJoinWithDefault = productStreamString.join(couponStreamString, valueJoinerString);
        KStream<String, JoinedClass> innerJoinWithKey = productStream.join(couponStream, valueJoinerWithKey, joined);
        KStream<String, JoinedClass> innerJoinWithDefaultWithKey = productStreamString.join(couponStreamString, valueJoinerWithKeyString);

        KStream<String, JoinedClass> leftJoin = productStream.leftJoin(couponStream, valueJoiner, joined);
        KStream<String, JoinedClass> leftJoinWithDefault = productStreamString.leftJoin(couponStreamString, valueJoinerString);
        KStream<String, JoinedClass> leftJoinWithKey = productStream.leftJoin(couponStream, valueJoinerWithKey, joined);
        KStream<String, JoinedClass> leftJoinWithDefaultWithKey = productStreamString.leftJoin(couponStreamString, valueJoinerWithKeyString);

        innerJoin.to("innerJoin", produced);
        innerJoinWithDefault.to("innerJoinWithDefault", produced);
        innerJoinWithKey.to("innerJoinWithKey", produced);
        innerJoinWithDefaultWithKey.to("innerJoinWithDefaultWithKey", produced);
        leftJoin.to("leftJoin", produced);
        leftJoinWithDefault.to("leftJoinWithDefault", produced);
        leftJoinWithKey.to("leftJoinWithKey", produced);
        leftJoinWithDefaultWithKey.to("leftJoinWithDefaultWithKey", produced);

        return builder.build();
    }
}
