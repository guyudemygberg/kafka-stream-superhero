package best.of.kafka.streams.processors.dto;

public class JoinedClass {

    private String name;
    private Integer discount;

    private Integer price;

    private Integer priceAfterDiscount;

    public Integer getPrice() {
        return price;
    }

    public Integer getPriceAfterDiscount() {
        return priceAfterDiscount;
    }

    public Integer getDiscount() {
        return discount;
    }

    public String getName() {
        return name;
    }
    private exProduct product;
    private exCoupon coupon;

    public JoinedClass(String name, Integer discount, Integer price, Integer priceAfterDiscount) {
        this.name = name;
        this.discount = discount;
        this.price = price;
        this.priceAfterDiscount = priceAfterDiscount;
    }

    public JoinedClass(String name, Integer discount) {
        this.name = name;
        this.discount = discount;
    }

    public JoinedClass(String name, String discount) {
        this.name = name;
        this.discount = discount == null ? 10 : Integer.valueOf(discount);
    }

    public JoinedClass(String key, String name, String discount) {
        this.name = name == null ? null : key + name;
        this.discount = discount == null ? 10 : Integer.valueOf(discount);
    }

    public JoinedClass(exProduct product, exCoupon coupon) {
        this.name = product == null ? null : product.getName();
        this.discount = coupon == null ? null : coupon.getDiscount();
    }

    public JoinedClass(String key, exProduct product, exCoupon coupon) {
        this.name = product == null ? null : (key + product.getName());
        this.discount = coupon == null ? null : coupon.getDiscount();
    }

//    public JoinedClass(String key, String name, String discount) {
//        this.name = name == null ? null : (key + name);
//        this.discount = discount == null ? 10 : Integer.valueOf(discount);
//    }




//    public JoinedClass(Product product, Coupon coupon) {
//        this.name = product.getName();
//        this.price = product.getPrice();
//        this.discount =  coupon == null ? null :coupon.getDiscount();
//        this.priceAfterDiscount = coupon == null || product.getPrice() == null ? null : product.getPrice() * coupon.getDiscount()/100;
//    }

//    public JoinedClass(String name, Product product, Coupon coupon) {
//        this.name = name;
//        this.price = product.getPrice();
//        this.discount =  coupon == null ? null :coupon.getDiscount();
//        this.priceAfterDiscount = coupon == null || product.getPrice() == null ? null : product.getPrice() * coupon.getDiscount()/100;
//    }
}