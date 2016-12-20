package esper54.domain;

/**
 * Created by liwangchun on 16/12/20.
 */
public class OrderHistory {
   private long orderId;
   private String his;

    public OrderHistory(String his, long orderId) {
        this.his = his;
        this.orderId = orderId;
    }

    public OrderHistory() {
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public String getHis() {
        return his;
    }

    public void setHis(String his) {
        this.his = his;
    }

    @Override
    public String toString() {
        return "OrderHistory{" +
                "orderId=" + orderId +
                ", his='" + his + '\'' +
                '}';
    }
}
