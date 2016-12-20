package esper54.subscriber;

/**
 * Created by liwangchun on 16/12/20.
 */
public class DefaultOrderEvent {
    // Called by the engine before delivering events to update methods
    public void updateStart(int insertStreamLength, int removeStreamLength){
        System.out.println("updateStart");
        System.out.println("insertStreamLength:"+insertStreamLength+":removeStreamLength:"+removeStreamLength);
    }
    // To deliver insert stream events
    public void update(long orderId, long count) {
        System.out.println("update orderId:"+orderId+":count:"+count);
    }
    // To deliver remove stream events
    public void updateRStream(long orderId, long count) {
        System.out.println("updateRStream orderId:"+orderId+":count:"+count);
    }
    // Called by the engine after delivering events
    public void updateEnd() {
        System.out.println("update End");
    }
}
