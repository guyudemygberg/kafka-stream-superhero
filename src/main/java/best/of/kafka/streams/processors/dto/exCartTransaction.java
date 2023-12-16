package best.of.kafka.streams.processors.dto;

public class exCartTransaction {

    private String cartId;
    private String productName;

    private Integer price;

    public Integer getPrice() {
        return price;
    }

    public String getCartId() {
        return cartId;
    }

    public String getProductName() {
        return productName;
    }
}
