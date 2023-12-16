package best.of.kafka.streams.dto;

public class Checkout {

    private String cartId;
    private String productName;
    private Integer price;
    private String customerName;
    private String transactionId;

    public Checkout(CartTransaction cartTransaction, Cart cart){
        this.cartId = cartTransaction.getCartId();
        this.productName = cartTransaction.getProductName();
        this.price = cartTransaction.getPrice();
        this.customerName = cart == null ? "Customer not found" : cart.getCustomerName();
    }

    public Checkout(String key, CartTransaction cartTransaction, Cart cart){
        this.cartId = cartTransaction.getCartId();
        this.productName = cartTransaction.getProductName();
        this.price = cartTransaction.getPrice();
        this.customerName = cart == null ? "Customer not found" : cart.getCustomerName();
        this.transactionId = key;
    }
}