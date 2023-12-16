package best.of.kafka.streams.processors.joins;

import best.of.kafka.streams.processors.dto.exCoupon;
import best.of.kafka.streams.processors.dto.JoinedClass;
import best.of.kafka.streams.processors.dto.exProduct;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Component;

//@Component
public class KTableToKTable {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, exProduct> productStream = builder.table("product", Consumed.with(Serdes.String(), SerdesUtils.getSerde(exProduct.class)));
        KTable<String, exCoupon> couponStream = builder.table("coupon", Consumed.with(Serdes.String(), SerdesUtils.getSerde(exCoupon.class)));
        Produced<String, JoinedClass> produced = Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class));
        ValueJoiner<exProduct, exCoupon,JoinedClass> valueJoiner = (product, coupon) -> new JoinedClass(product, coupon);
        Materialized<String, JoinedClass, KeyValueStore<Bytes, byte[]>> materialized = Materialized.as("sjs").with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class));

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
//        KStream<String, JoinedClass> valueJoinerNoDefault =   productStream.join(couponStream, (product, coupon) -> new JoinedClass(product.getName(), coupon.getDiscount()), windows, streamJoined);
//        KStream<String, JoinedClass> valueJoinerWithDefault = productStringStream.join(couponStreamString, (product, coupon) -> new JoinedClass(product, Integer.valueOf(coupon)), windows);
//        KStream<String, JoinedClass> keyValueJoinerNoDefault =   productStream.join(couponStream, (key, product, coupon) -> new JoinedClass(key + product.getName(), coupon.getDiscount()), windows, streamJoined);
//        KStream<String, JoinedClass> keyValueJoinerWithDefault = productStringStream.join(couponStreamString, (key, product, coupon) -> new JoinedClass(key + product, Integer.valueOf(coupon)), windows);

//        KStream<String, JoinedClass> leftValueJoinerNoDefault =   productStream.leftJoin(couponStream, (product, coupon) -> new JoinedClass(product.getName(), coupon.getDiscount()), windows, streamJoined);
//        KStream<String, JoinedClass> leftValueJoinerWithDefault = productStringStream.leftJoin(couponStreamString, (product, coupon) -> new JoinedClass(product, Integer.valueOf(coupon)), windows);
//        KStream<String, JoinedClass> leftKeyValueJoinerNoDefault =   productStream.leftJoin(couponStream, (key, product, coupon) -> new JoinedClass(key + product.getName(), coupon.getDiscount()), windows, streamJoined);
//        KStream<String, JoinedClass> leftKeyValueJoinerWithDefault = productStringStream.leftJoin(couponStreamString, (key, product, coupon) -> new JoinedClass(key + product, Integer.valueOf(coupon)), windows);

//        KStream<String, JoinedClass> outerValueJoinerNoDefault =   productStream.outerJoin(couponStream, (product, coupon) -> new JoinedClass(product.getName(), coupon.getDiscount()), windows, streamJoined);
//        KStream<String, JoinedClass> outerValueJoinerWithDefault = productStringStream.outerJoin(couponStreamString, (product, coupon) -> new JoinedClass(product, Integer.valueOf(coupon)), windows);
//        KStream<String, JoinedClass> outerKeyValueJoinerNoDefault =   productStream.outerJoin(couponStream, (key, product, coupon) -> new JoinedClass(key + product.getName(), coupon.getDiscount()), windows, streamJoined);
//        KStream<String, JoinedClass> outerKeyValueJoinerWithDefault = productStringStream.outerJoin(couponStreamString, (key, product, coupon) -> new JoinedClass(key + product, Integer.valueOf(coupon)), windows);

//        valueJoinerNoDefault.to("innerJoin-ValueJoiner", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
//        valueJoinerWithDefault.to("innerJoin-ValueJoiner-default", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
//        keyValueJoinerNoDefault.to("innerJoin-KeyValueJoiner", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
//        keyValueJoinerWithDefault.to("innerJoin-KeyValueJoiner-default", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
//        leftValueJoinerNoDefault.to("leftJoin-ValueJoiner", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
//        leftValueJoinerWithDefault.to("leftJoin-ValueJoiner-default", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
//        leftKeyValueJoinerNoDefault.to("leftJoin-KeyValueJoiner", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
//        leftKeyValueJoinerWithDefault.to("leftJoin-KeyValueJoiner-default", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
//        outerValueJoinerNoDefault.to("outerJoin-ValueJoiner", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
//        outerValueJoinerWithDefault.to("outerJoin-ValueJoiner-default", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
//        outerKeyValueJoinerNoDefault.to("outerJoin-KeyValueJoiner", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
//        outerKeyValueJoinerWithDefault.to("outerJoin-KeyValueJoiner-default", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));

        return builder.build();
    }
}