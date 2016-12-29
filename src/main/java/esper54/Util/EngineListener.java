package esper54.Util;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceStateListener;

import javax.sound.midi.Soundbank;

/**
 * Created by liwangchun on 16/12/29.
 */
public class EngineListener implements EPServiceStateListener {
    public void onEPServiceDestroyRequested(EPServiceProvider serviceProvider) {
        System.out.println("destroy before");
        System.out.println(serviceProvider);
    }

    public void onEPServiceInitialized(EPServiceProvider serviceProvider) {
        System.out.println("initialize after");
        System.out.println(serviceProvider);
    }
}
