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
public class KStreamToGlobal {

//    @Bean
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, exCartTransaction> productStream = builder.stream("cart-transaction", Consumed.with(Serdes.String(), SerdesUtils.getSerde(exCartTransaction.class)));
        GlobalKTable<String, exCart> couponStream = builder.globalTable("cart22", Consumed.with(Serdes.String(), SerdesUtils.getSerde(exCart.class)));
        Produced<String, reCheckout> produced = Produced.with(Serdes.String(), SerdesUtils.getSerde(reCheckout.class));
        KeyValueMapper<String, exCartTransaction, String> keyValueMapper = (cartTranKey, cartTranValue) -> cartTranValue.getCartId();

        ValueJoiner<exCartTransaction, exCart, reCheckout> valueJoiner = (cartTran, cart) -> new reCheckout(cartTran, cart);
        ValueJoinerWithKey<String, exCartTransaction, exCart, reCheckout> valueJoinerWithKey = (key, tran, cart) -> new reCheckout(key, tran, cart);

        KStream<String, reCheckout> innerValueJoin = productStream.join(couponStream, keyValueMapper, valueJoiner);
        KStream<String, reCheckout> innerValueJoinWithKey = productStream.join(couponStream, keyValueMapper, valueJoinerWithKey);
        KStream<String, reCheckout> innerValueJoinWithName = productStream.join(couponStream, keyValueMapper, valueJoiner, Named.as("inner-valueJoiner"));
        KStream<String, reCheckout> innerValueJoinWithKeyWithName = productStream.join(couponStream, keyValueMapper, valueJoinerWithKey, Named.as("inner-valueJoinerWithKey"));
        KStream<String, reCheckout> leftValueJoin = productStream.leftJoin(couponStream, keyValueMapper, valueJoiner);
        KStream<String, reCheckout> leftValueJoinWithKey = productStream.leftJoin(couponStream, keyValueMapper, valueJoinerWithKey);
        KStream<String, reCheckout> leftValueJoinWithName = productStream.leftJoin(couponStream, keyValueMapper, valueJoiner, Named.as("left-valueJoiner"));
        KStream<String, reCheckout> leftValueJoinWithKeyWithName = productStream.leftJoin(couponStream, keyValueMapper, valueJoinerWithKey, Named.as("left-valueJoinerWithKey"));


//

        innerValueJoin.to("innerJoin-ValueJoiner", produced);
        innerValueJoinWithKey.to("innerJoin-ValueJoiner-default", produced);
        innerValueJoinWithName.to("innerJoin-KeyValueJoiner", produced);
        innerValueJoinWithKeyWithName.to("innerJoin-KeyValueJoiner-default", produced);
        leftValueJoin.to("leftJoin-ValueJoiner", produced);
        leftValueJoinWithKey.to("leftJoin-ValueJoiner-default", produced);
        leftValueJoinWithName.to("leftJoin-KeyValueJoiner", produced);
        leftValueJoinWithKeyWithName.to("leftJoin-KeyValueJoiner-default", produced);


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