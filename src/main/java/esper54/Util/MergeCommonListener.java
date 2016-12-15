package esper54.Util;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.client.UpdateListener;

/**
 * Created by liwangchun on 16/12/6.
 */
public class MergeCommonListener implements UpdateListener {
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents!=null){
            System.out.println("           merge new events");
            soutEventMessage(newEvents);
        }
        if (oldEvents!=null){
            System.out.println("           merge old events");
            soutEventMessage(oldEvents);
        }
    }
    private void soutEventMessage(EventBean[] events){
        System.out.println("merge Events Length:" + events.length);
        for (int i=0;i<events.length;i++)
        {
            EventType eventType = events[0].getEventType();
            String[] properties = eventType.getPropertyNames();
            if (i==0){
                System.out.print("merge event " + i + " properties name:");
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
            System.out.println("merge underlying:"+events[i].getUnderlying());
        }
    }
}

