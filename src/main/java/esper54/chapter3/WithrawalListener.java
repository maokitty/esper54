package esper54.chapter3;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.client.UpdateListener;

/**
 * Created by liwangchun on 16/10/29.
 */
public class WithrawalListener implements UpdateListener {
    /**
     * @param newEvents
     * <p>getEventType:返回事件所有属性的集合，用来描述这个属性的类</p>
     * <p>getUnderlying:返回事件实例</p>
     * <p>get:返回事件属性</p>
     * @param oldEvents
     */
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents != null) {
            System.out.println("        new events");
            soutEventMessage(newEvents);
        }
        if (oldEvents != null ){
            System.out.println("        old events");
            soutEventMessage(oldEvents);
        }
    }
    private void soutEventMessage(EventBean[] events){
        System.out.println("Events Length:"+events.length);
        EventType eventType = events[0].getEventType();
        String[] properties = eventType.getPropertyNames();
        System.out.print("properties name:");
        for (int i=0;properties!=null && i<properties.length;i++)
        {
            if (i!=properties.length-1)
            {
                System.out.print(properties[i]+",");
            }else{
                System.out.println(properties[i]);
            }

        }
        Withdrawal event = (Withdrawal) events[0].getUnderlying();
        System.out.println("by java bean amount:" + event.getAmount()+";account:"+event.getAccount());
        System.out.println("by get method only amount:" + events[0].get("amount")+";account:"+events[0].get("account"));
    }
}
