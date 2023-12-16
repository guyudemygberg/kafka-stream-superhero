package best.of.kafka.streams.processors.joins;

import best.of.kafka.streams.processors.dto.exCoupon;
import best.of.kafka.streams.processors.dto.JoinedClass;
import best.of.kafka.streams.processors.dto.exProduct;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;

//@Component
public class KStreamToKStream {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, exProduct> productStream = builder.stream("product", Consumed.with(Serdes.String(), SerdesUtils.getSerde(exProduct.class)));
        KStream<String, exCoupon> couponStream = builder.stream("coupon", Consumed.with(Serdes.String(), SerdesUtils.getSerde(exCoupon.class)));
        KStream<String,String> productStringStream = productStream.mapValues((product) -> product.getName());
        KStream<String,String> couponStreamString = couponStream.mapValues((coupon) -> coupon.getDiscount()+"");

        JoinWindows windows = JoinWindows.of(Duration.ofDays(5));
        StreamJoined streamJoined = StreamJoined.with(Serdes.String(), SerdesUtils.getSerde(exProduct.class), SerdesUtils.getSerde(exCoupon.class));
        Produced<String, JoinedClass> produced = Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class));

        ValueJoiner<exProduct, exCoupon,JoinedClass> valueJoiner = (product, coupon) -> new JoinedClass(product, coupon);
        ValueJoinerWithKey<String, exProduct, exCoupon,JoinedClass> valueJoinerWithKey = (key, product, coupon) -> new JoinedClass(key, product, coupon);
        ValueJoiner<String, String,JoinedClass> valueJoinerString =  (product, coupon) -> new JoinedClass(product, coupon);
        ValueJoinerWithKey<String,String, String,JoinedClass> valueJoinerWithKeySting = (key, product, coupon) -> new JoinedClass(key, product, coupon);

        KStream<String, JoinedClass> innerJoin = productStream.join(couponStream, valueJoiner, windows, streamJoined);
        KStream<String, JoinedClass> innerJoinWithDefault = productStringStream.join(couponStreamString, valueJoinerString, windows);
        KStream<String, JoinedClass> innerJoinWithKey = productStream.join(couponStream, valueJoinerWithKey, windows, streamJoined);
        KStream<String, JoinedClass> innerJoinWithDefaultWithKey = productStringStream.join(couponStreamString, valueJoinerWithKeySting, windows);

        KStream<String, JoinedClass> leftJoin = productStream.leftJoin(couponStream, valueJoiner, windows, streamJoined);
        KStream<String, JoinedClass> leftJoinWithDefault = productStringStream.leftJoin(couponStreamString, valueJoinerString, windows);
        KStream<String, JoinedClass> leftJoinWithKey = productStream.leftJoin(couponStream, valueJoinerWithKey, windows, streamJoined);
        KStream<String, JoinedClass> leftJoinWithDefaultWithKey = productStringStream.leftJoin(couponStreamString, valueJoinerWithKeySting, windows);

        KStream<String, JoinedClass> outerJoin = productStream.outerJoin(couponStream, valueJoiner, windows, streamJoined);
        KStream<String, JoinedClass> outerJoinWithDefault = productStringStream.outerJoin(couponStreamString, valueJoinerString, windows);
        KStream<String, JoinedClass> outerJoinWithKey = productStream.outerJoin(couponStream, valueJoinerWithKey, windows, streamJoined);
        KStream<String, JoinedClass> outerJoinWithDefaultWithKey = productStringStream.outerJoin(couponStreamString, valueJoinerWithKeySting, windows);

        innerJoin.to("innerJoin-Join", produced);
        innerJoinWithDefault.to("innerJoin-Join-default", produced);
        innerJoinWithKey.to("innerJoin-Join", produced);
        innerJoinWithDefaultWithKey.to("innerJoin-Join-WithKeydefault", produced);
        leftJoin.to("leftJoin-Join", produced);
        leftJoinWithDefault.to("leftJoin-Join-default", produced);
        leftJoinWithKey.to("leftJoin-Join", produced);
        leftJoinWithDefaultWithKey.to("leftJoin-Join-WithKeydefault", produced);
        outerJoin.to("outerJoin-Join", produced);
        outerJoinWithDefault.to("outerJoin-Join-default", produced);
        outerJoinWithKey.to("outerJoin-Join", produced);
        outerJoinWithDefaultWithKey.to("outerJoin-Join-WithKeydefault", produced);


//        KStream<String, JoinedClass> valueJoiner =   productStream.join(couponStream, (product, coupon) -> new JoinedClass(product.getName(), coupon.getDiscount()), windows, streamJoined);
//        KStream<String, JoinedClass> valueJoinerWithDefault = productStringStream.join(couponStreamString, (product, coupon) -> new JoinedClass(product, Integer.valueOf(coupon)), windows);
//        KStream<String, JoinedClass> keyJoinNoDefault =   productStream.join(couponStream, (key, product, coupon) -> new JoinedClass(key + product.getName(), coupon.getDiscount()), windows, streamJoined);
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