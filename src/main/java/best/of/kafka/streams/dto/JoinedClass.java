package best.of.kafka.streams.dto;

public class JoinedClass {

    private String name;
    private Integer price;
    private Integer discount;

    public Integer getDiscount() {
        return discount;
    }

    public String getName() {
        return name;
    }

    public Integer getPrice() {
        return price;
    }

    public JoinedClass(Product product, Coupon coupon){
        this.name = product == null ? null : product.getName();
        this.price = product == null ? null : product.getPrice();
        this.discount = coupon == null ? null : coupon.getDiscount();
    }

    public JoinedClass(String key, Product product, Coupon coupon){
        this.name = product == null ? null : (key + product.getName());
        this.price = product == null ? null : product.getPrice();
        this.discount = coupon == null ? null : coupon.getDiscount();
    }

    public JoinedClass(String name, String discount){
        this.name = name;
        this.discount = discount == null ? 0 : Integer.valueOf(discount);
    }

    public JoinedClass(String key, String name, String discount){
        this.name = name == null ? null : (key + name);
        this.discount = discount == null ? 0 : Integer.valueOf(discount);
    }
}
