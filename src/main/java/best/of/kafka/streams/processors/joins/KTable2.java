package best.of.kafka.streams.processors.joins;

import best.of.kafka.streams.processors.dto.exCoupon;
import best.of.kafka.streams.processors.dto.JoinedClass;
import best.of.kafka.streams.processors.dto.exProduct;
import best.of.kafka.streams.utils.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

//@Component
public class KTable2 {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, exProduct> productStream = builder.stream("product", Consumed.with(Serdes.String(), SerdesUtils.getSerde(exProduct.class)));
        KTable<String, exCoupon> couponStream = builder.table("coupon", Consumed.with(Serdes.String(), SerdesUtils.getSerde(exCoupon.class)));
        KStream<String,String> productStringStream = productStream.mapValues((product) -> product.getName());
        KTable<String,String> couponStreamString = couponStream.mapValues((coupon) -> coupon.getDiscount()+"");
        Joined joined = Joined.with(Serdes.String(), SerdesUtils.getSerde(exProduct.class), SerdesUtils.getSerde(exCoupon.class));
        Produced<String, JoinedClass> produced = Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class));

        ValueJoiner<exProduct, exCoupon,JoinedClass> valueJoiner = (product, coupon) -> new JoinedClass(product, coupon);
        ValueJoinerWithKey<String, exProduct, exCoupon,JoinedClass> valueJoinerWithKey = (key, product, coupon) -> new JoinedClass(key, product, coupon);
        ValueJoiner<String, String,JoinedClass> valueJoinerString =  (product, coupon) -> new JoinedClass(product, coupon);
        ValueJoinerWithKey<String,String, String,JoinedClass> valueJoinerWithKeySting = (key, product, coupon) -> new JoinedClass(key, product, coupon);

        KStream<String, JoinedClass> innerJoin = productStream.join(couponStream, valueJoiner, joined);
        KStream<String, JoinedClass> innerValueJoinerWithDefault = productStringStream.join(couponStreamString, valueJoinerString);
        KStream<String, JoinedClass> innerKeyValueJoinerNoDefault = productStream.join(couponStream, valueJoinerWithKey, joined);
        KStream<String, JoinedClass> innerKeyValueJoinerWithDefault = productStringStream.join(couponStreamString, valueJoinerWithKeySting);

        KStream<String, JoinedClass> leftValueJoinerNoDefault = productStream.leftJoin(couponStream, valueJoiner, joined);
        KStream<String, JoinedClass> leftValueJoinerWithDefault = productStringStream.leftJoin(couponStreamString, valueJoinerString);
        KStream<String, JoinedClass> leftKeyValueJoinerNoDefault = productStream.leftJoin(couponStream, valueJoinerWithKey, joined);
        KStream<String, JoinedClass> leftKeyValueJoinerWithDefault = productStringStream.leftJoin(couponStreamString, valueJoinerWithKeySting);

        innerJoin.to("innerJoin-ValueJoiner", produced);
        innerValueJoinerWithDefault.to("innerJoin-ValueJoiner-default", produced);
        innerKeyValueJoinerNoDefault.to("innerJoin-KeyValueJoiner", produced);
        innerKeyValueJoinerWithDefault.to("innerJoin-KeyValueJoiner-default", produced);
        leftValueJoinerNoDefault.to("leftJoin-ValueJoiner", produced);
        leftValueJoinerWithDefault.to("leftJoin-ValueJoiner-default", produced);
        leftKeyValueJoinerNoDefault.to("leftJoin-KeyValueJoiner", produced);
        leftKeyValueJoinerWithDefault.to("leftJoin-KeyValueJoiner-default", produced);


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