package esper54.subscriber;

import esper54.domain.Order;
import esper54.domain.OrderHistory;

import java.util.Map;

/**
 * Created by liwangchun on 16/12/20.
 */
public class OrderEvent {
    public void orderIn(long orderId,double price ,long count){
        System.out.println("orderId:"+orderId+":price:"+price+":count:"+count);
    }
    public void orderAndHistoryJoin(Order order,OrderHistory history,long count){
        System.out.println("order:"+order+":history:"+history+":count:"+count);
    }
    public void orderMap(Map row){
        System.out.println("orderId:"+row.get("orderId"));
    }
}
