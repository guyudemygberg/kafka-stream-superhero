package best.of.kafka.streams.dto;

public class CartTransaction {

    private String cartId;
    private String productName;
    private Integer price;

    public String getCartId() {
        return cartId;
    }

    public String getProductName() {
        return productName;
    }

    public Integer getPrice() {
        return price;
    }
}
