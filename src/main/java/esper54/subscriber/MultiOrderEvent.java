package esper54.subscriber;

import esper54.domain.Order;

/**
 * Created by liwangchun on 16/12/20.
 */
public class MultiOrderEvent {
    public void update(Order[] insertStream, Order[] removeStream) {
        System.out.println("insert start");
        printOrder(insertStream);
        System.out.println("remove start");
        printOrder(removeStream);
    }
    private void printOrder(Order[] orders){
        if (orders!=null){
            System.out.println("print start");
            for (Order order:orders){
                System.out.println(order);
            }
            System.out.println("print end");
        }
    }
}
