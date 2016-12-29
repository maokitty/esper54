package esper54.Util;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EPStatementStateListener;

/**
 * Created by liwangchun on 16/12/29.
 */
public class EngineStatementListener implements EPStatementStateListener {
    public void onStatementCreate(EPServiceProvider serviceProvider, EPStatement statement) {
        System.out.println("statement "+statement.getName()+" create");
    }

    public void onStatementStateChange(EPServiceProvider serviceProvider, EPStatement statement) {
        System.out.println("statement " + statement.getName() + " state change state " + statement.getState());

    }
}
