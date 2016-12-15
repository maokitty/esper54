package esper54.Util;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.client.UpdateListener;

/**
 * Created by liwangchun on 16/12/5.
 */
public class UpdateCommonListener implements UpdateListener{
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        System.out.println("newEvents:"+newEvents+";    oldEvents:"+oldEvents);
        if (newEvents!=null){
            System.out.println("           update new events");
            soutEventMessage(newEvents);
        }
        if (oldEvents!=null){
            System.out.println("           update old events");
            soutEventMessage(oldEvents);
        }
    }
    private void soutEventMessage(EventBean[] events){
        System.out.println("update Events Length:" + events.length);
        for (int i=0;i<events.length;i++)
        {
            EventType eventType = events[0].getEventType();
            String[] properties = eventType.getPropertyNames();
            if (i==0){
                System.out.print("update event " + i + " properties name:");
                for (int j=0;properties!=null && j<properties.length;j++)
                {
                    if (j!=properties.length-1)
                    {
                        System.out.print(properties[j]+",");
                    }else{
                        System.out.println(properties[j]);
                    }

                }
            }
            System.out.println("update underlying:"+events[i].getUnderlying());
        }
    }
}
