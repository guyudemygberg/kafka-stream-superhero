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
public class KStreamToKTable {

//    @Bean
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, exProduct> productStream = builder.stream("product", Consumed.with(Serdes.String(), SerdesUtils.getSerde(exProduct.class)));
        KTable<String, exCoupon> couponStream = builder.table("coupon", Consumed.with(Serdes.String(), SerdesUtils.getSerde(exCoupon.class)));
        KStream<String, String> productStringStream = productStream.mapValues((product) -> product.getName());
        KTable<String, String> couponStringStream = couponStream.mapValues((coupon) -> coupon.getDiscount() + "");
        Joined joined = Joined.with(Serdes.String(), SerdesUtils.getSerde(exProduct.class), SerdesUtils.getSerde(exCoupon.class));

        ValueJoiner<exProduct, exCoupon,JoinedClass> valueJoiner = (product, coupon) -> new JoinedClass(product, coupon);
        ValueJoinerWithKey<String, exProduct, exCoupon,JoinedClass> valueJoinerWithKey = (key, product, coupon) -> new JoinedClass(key, product, coupon);
        ValueJoiner<String, String,JoinedClass> valueJoinerString =  (product, coupon) -> new JoinedClass(product, coupon);
        ValueJoinerWithKey<String,String, String,JoinedClass> valueJoinerWithKeySting = (key, product, coupon) -> new JoinedClass(key, product, coupon);

        KStream<String, JoinedClass> valueJoinerNoDefault =   productStream.join(couponStream, valueJoiner, joined);
        KStream<String, JoinedClass> valueJoinerWithDefault = productStringStream.join(couponStringStream, valueJoinerString);
        KStream<String, JoinedClass> keyValueJoinerNoDefault =   productStream.join(couponStream,valueJoinerWithKey, joined);
        KStream<String, JoinedClass> keyValueJoinerWithDefault = productStringStream.join(couponStringStream, valueJoinerWithKeySting);

        KStream<String, JoinedClass> leftValueJoinerNoDefault =   productStream.leftJoin(couponStream, valueJoiner, joined);
        KStream<String, JoinedClass> leftValueJoinerWithDefault = productStringStream.leftJoin(couponStringStream, valueJoinerString);
        KStream<String, JoinedClass> leftKeyValueJoinerNoDefault =   productStream.leftJoin(couponStream, valueJoinerWithKey, joined);
        KStream<String, JoinedClass> leftKeyValueJoinerWithDefault = productStringStream.leftJoin(couponStringStream, valueJoinerWithKeySting);



        valueJoinerNoDefault.to("innerJoin-ValueJoiner", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
        valueJoinerWithDefault.to("innerJoin-ValueJoiner-default", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
        keyValueJoinerNoDefault.to("innerJoin-KeyValueJoiner", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
        keyValueJoinerWithDefault.to("innerJoin-KeyValueJoiner-default", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
        leftValueJoinerNoDefault.to("leftJoin-ValueJoiner", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
        leftValueJoinerWithDefault.to("leftJoin-ValueJoiner-default", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
        leftKeyValueJoinerNoDefault.to("leftJoin-KeyValueJoiner", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));
        leftKeyValueJoinerWithDefault.to("leftJoin-KeyValueJoiner-default", Produced.with(Serdes.String(), SerdesUtils.getSerde(JoinedClass.class)));

        return builder.build();
    }
}
