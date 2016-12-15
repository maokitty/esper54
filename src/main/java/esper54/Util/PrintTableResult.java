package esper54.Util;

import com.espertech.esper.client.EPOnDemandQueryResult;
import com.espertech.esper.client.EventBean;

/**
 * Created by liwangchun on 16/12/6.
 */
public class PrintTableResult {
    public static void execute(EPOnDemandQueryResult result){
        if (result!=null){
            EventBean[] eventBeans=result.getArray();
            if (eventBeans!=null)
            {
                for (int i=0;i<eventBeans.length;i++){
                    EventBean bean=eventBeans[i];
                    String[] properties=bean.getEventType().getPropertyNames();
                    if (i==0){
                        System.out.print("table event " + i + " properties name:");
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
                    StringBuilder outBuilder=new StringBuilder("underlying:");
                    for (int j=0;j<properties.length;j++){
                        outBuilder.append(eventBeans[i].get(properties[j]).toString());
                        if (j!=properties.length-1)
                        {
                            outBuilder.append("\t");
                        }
                    }
                    System.out.println(outBuilder.toString());
                }
            }else {
                System.out.println("result.getArray is null");
            }
        }else {
            System.out.println("table result is null");
        }
    }
}
