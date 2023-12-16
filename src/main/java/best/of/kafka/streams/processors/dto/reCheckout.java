package best.of.kafka.streams.processors.dto;

public class reCheckout {
    private String cartId;
    private String customerName;

    private String productName;
    private String tranId;
    private Integer price;
    private exCartTransaction cartTransaction;
    private exCart cart;

//    public Checkout(CartTransaction cartTransaction, Cart cart){
////        this.cartId = cartTransaction.getCartId();
////        this.price = cartTransaction.getPrice();
////        this.productName = cartTransaction.getProductName();
//        this.cartTransaction = cartTransaction;
//        this.customerName = cart == null ? "No customer found" : cart.getCustomerName();
//    }

    public reCheckout(exCartTransaction cartTransaction, exCart cart) {
        this.cartTransaction = cartTransaction;
        this.cart = cart;
    }

    public reCheckout(String tranId, exCartTransaction cartTransaction, exCart cart){
        this.cartId = cartTransaction.getCartId();
        this.price = cartTransaction.getPrice();
        this.tranId = tranId;
        this.customerName = cart == null ? "No customer found" : cart.getCustomerName();
    }
}
