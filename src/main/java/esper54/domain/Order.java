package esper54.domain;

/**
 * Created by liwangchun on 16/11/15.
 */
public class Order {
    private long orderId;
    private String cosumerName;
    private long volume;
    private double price;

    public Order(long orderId, String cosumerName, long volume, double price, String symbol) {
        this.orderId = orderId;
        this.cosumerName = cosumerName;
        this.volume = volume;
        this.price = price;
        this.symbol = symbol;
    }

    public Order() {
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId=" + orderId +
                ", cosumerName='" + cosumerName + '\'' +
                ", volume=" + volume +
                ", price=" + price +
                ", symbol='" + symbol + '\'' +
                '}';
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public long getVolume() {
        return volume;
    }

    public void setVolume(long volume) {
        this.volume = volume;
    }

    private String symbol;

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public String getCosumerName() {
        return cosumerName;
    }

    public void setCosumerName(String cosumerName) {
        this.cosumerName = cosumerName;
    }
}
