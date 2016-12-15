package esper54.chapter5;

import com.espertech.esper.client.*;

import java.lang.annotation.Annotation;

/**
 * Created by liwangchun on 16/10/31.
 */
public class AnnotionFunc {
    public static void main(String[] args) {
        EPServiceProvider provider = EPServiceProviderManager.getDefaultProvider();
        String eventName = ProcessEvent.class.getName();
        segment5271(provider.getEPAdministrator(),provider.getEPRuntime(),eventName);

    }

    /**
     * todo 不明白这个注解有什么用
     * @param admin
     * @param runtime
     * @param eventName
     */

    public static void segment5271(EPAdministrator admin,EPRuntime runtime,String eventName){
        admin.getConfiguration().addAnnotationImport("esper54.chapter5.ProcessMonitor");
        StringBuilder pBuilder = new StringBuilder("@ProcessMonitor(processName=\"aaa\",isLongRunning=true,subProcessIds={1,2,3}) select irstream count(*),processName from ");
        pBuilder.append(eventName);
        pBuilder.append(" (processId in (1,2,3)).win:time(5 sec)");
        String processEvent = pBuilder.toString();
        EPStatement statement = admin.createEPL(processEvent);
        for (Annotation a:statement.getAnnotations()){
            System.out.println(a.toString());
        }
        statement.addListener(new SelfDefineAnnotionListener());
        ProcessEvent event0 = new ProcessEvent();
        event0.setIsLongRunning(false);
        event0.setProcessId(0);
        event0.setProcessName("maokitty0");
        runtime.sendEvent(event0);
        ProcessEvent event1 = new ProcessEvent();
        event1.setIsLongRunning(true);
        event1.setProcessId(1);
        event1.setProcessName("maokitty1");
        runtime.sendEvent(event1);
        ProcessEvent event2 = new ProcessEvent();
        event2.setIsLongRunning(false);
        event2.setProcessId(2);
        event2.setProcessName("maokitty2");
        runtime.sendEvent(event2);
        ProcessEvent event3 = new ProcessEvent();
        event3.setIsLongRunning(true);
        event3.setProcessId(3);
        event3.setProcessName("maokitty3");
        runtime.sendEvent(event3);
        ProcessEvent event4 = new ProcessEvent();
        event4.setIsLongRunning(true);
        event4.setProcessId(4);
        event4.setProcessName("maokitty4");
        runtime.sendEvent(event4);
    }
}

