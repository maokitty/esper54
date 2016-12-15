package esper54.chapter5;

/**
 * Created by liwangchun on 16/10/31.
 */
public @interface ProcessMonitor {
    String processName();
    boolean isLongRunning() default false;
    int[] subProcessIds();
}
