package esper54.domain;

/**
 * Created by liwangchun on 16/10/31.
 */
public class Process {
    public String getProcessName() {
        return processName;
    }

    public void setProcessName(String processName) {
        this.processName = processName;
    }

    private String processName;

    public boolean isLongRunning() {
        return isLongRunning;
    }

    public void setIsLongRunning(boolean isLongRunning) {
        this.isLongRunning = isLongRunning;
    }

    boolean isLongRunning;

    public int getProcessId() {
        return processId;
    }

    public void setProcessId(int processId) {
        this.processId = processId;
    }

    int processId;
}
