package esper54.Util;

import java.util.concurrent.TimeUnit;

/**
 * Created by liwangchun on 16/11/15.
 */
public class TimeUtil {
    public static void sleepSec(int sec){
        try {
            System.out.println("    sleep "+sec+" sec start");
            TimeUnit.SECONDS.sleep(sec);
            System.out.println("    sleep over");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
