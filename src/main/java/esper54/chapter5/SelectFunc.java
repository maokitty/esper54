package esper54.chapter5;

import com.espertech.esper.client.*;
import esper54.Util.CommonListener;

import java.util.concurrent.TimeUnit;

/**
 * Created by liwangchun on 16/10/31.
 */
public class SelectFunc {
    public static void main(String[] args) {
        EPServiceProvider provider = EPServiceProviderManager.getDefaultProvider();
        String eventName = StockTick.class.getName();
//        segment535(provider.getEPAdministrator(), provider.getEPRuntime(), eventName);
        segment539(provider.getEPAdministrator(), provider.getEPRuntime(), eventName);
    }



    /**
     * 形如 select irstream distinct tick.symbol as symbol, tick.price price from StockTick.win:time(4) as tick</p>
     * 1:distinct必须在select之后出现,调换symbol和price后会编译出错</p>
     * 2:distinct在这种场景下并不生效,这里每次只输出一个，只有在有两个或之上的输出事件才会触发去重,改为time_batch生效</p>
     *
     * @param admin
     * @param runtime
     * @param eventName
     */
    public static void segment539(EPAdministrator admin,EPRuntime runtime,String eventName){
        StringBuilder pBuilder = new StringBuilder("select irstream distinct tick.symbol as symbol, tick.price price from ");
        pBuilder.append(eventName);
//        pBuilder.append(".win:time(4) as tick"); //观察time_batch与此的区别
        pBuilder.append(".win:time_batch(4 sec) as tick");
        String processEvent = pBuilder.toString();
        EPStatement statement = admin.createEPL(processEvent);
        statement.addListener(new CommonListener());
        StockTick tick0 = new StockTick();
        tick0.setPrice(1);
        tick0.setSymbol("maokitty0");
        runtime.sendEvent(tick0);
        StockTick tick1 = new StockTick();
        tick1.setPrice(2);
        tick1.setSymbol("maokitty1");
        runtime.sendEvent(tick1);
        runtime.sendEvent(tick1);
        runtime.sendEvent(tick1);
        sleep5Sec();
    }

    /**
     * 形如 select irstream tick.price price,tick.symbol as symbol,news.text as text from Stock.win:time(4)  as tick,News.win:time(4) as news where tick.symbol=news.symbol</p>
     * 1：对时间窗口内的数据做聚合,可有各自的时间窗口长度[添加tick失效后再次发送并改变joinEvent长度为10 观察]</p>
     * 2：聚合的只有一种时间传入不会触发listener[注释掉 send news观察]</p>
     * @param admin
     * @param runtime
     * @param eventName
     */
    public static void segment535(EPAdministrator admin,EPRuntime runtime,String eventName){
        String joinEvent = News.class.getName();
        StringBuilder pBuilder = new StringBuilder("select irstream tick.price price,tick.symbol as symbol,news.text as text from ");
        pBuilder.append(eventName);
        pBuilder.append(".win:time(4) as tick,");
        pBuilder.append(joinEvent);
        pBuilder.append(".win:time(4) as news where tick.symbol=news.symbol");
        String processEvent = pBuilder.toString();
        EPStatement statement = admin.createEPL(processEvent);
        statement.addListener(new CommonListener());
        StockTick tick0 = new StockTick();
        tick0.setPrice(1);
        tick0.setSymbol("maokitty0");
        runtime.sendEvent(tick0);
        StockTick tick1 = new StockTick();
        tick1.setPrice(2);
        tick1.setSymbol("maokitty1");
        runtime.sendEvent(tick1);
//        sleep5Sec(); //观察只有news事件
        News news0 = new News();
        news0.setSymbol("maokitty0");
        news0.setText("maokitty0的新闻");
        runtime.sendEvent(news0);
        News news1 = new News();
        news1.setSymbol("maokitty0");
        news1.setText("maokitty0的另一条新闻");
        runtime.sendEvent(news1);
        sleep5Sec();
        News news2 = new News();
        news2.setSymbol("maokitty0");
        news2.setText("maokitty05s后的一条新闻");
        runtime.sendEvent(news2);
//        runtime.sendEvent(tick0);//观察时间窗口长度不一致
        sleep5Sec();
    }

    private static void sleep5Sec(){
        try {
            System.out.println("    sleep 5 sec start");
            TimeUnit.SECONDS.sleep(5);
            System.out.println("    sleep over");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
