package esper54.Util;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.client.UpdateListener;

/**
 * Created by liwangchun on 16/12/5.
 */
public class SchemaCommonListener implements UpdateListener {
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents!=null){
            System.out.println("           schema batch new events");
            soutEventMessage(newEvents);
        }
        if (oldEvents!=null){
            System.out.println("           schema batch old events");
            soutEventMessage(oldEvents);
        }
    }
    private void soutEventMessage(EventBean[] events){
        System.out.println("schema Events Length:" + events.length);

        for (int i=0;i<events.length;i++)
        {
            EventType eventType = events[0].getEventType();
            String[] properties = eventType.getPropertyNames();
            if (i==0){

                System.out.print("schema event " + i + " properties name:");
                for (int j=0;properties!=null && j<properties.length;j++)
                {
                    if (j!=properties.length-1)
                    {
                        System.out.print(properties[j] + ",");

                    }else{

                        System.out.println(properties[j]);
                    }
                }
            }
            StringBuilder outBuilder=new StringBuilder("schema underlying:");
            for (int j=0;j<properties.length;j++){
                outBuilder.append(events[i].get(properties[j]).toString());
                if (j!=properties.length-1)
                {
                    outBuilder.append("\t");
                }
            }
            System.out.println(outBuilder.toString());
        }
    }
}
