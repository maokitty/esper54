package esper54.chapter5;

import com.espertech.esper.client.*;
import esper54.Util.*;
import esper54.domain.*;
import esper54.domain.Process;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 * Created by liwangchun on 16/11/15.
 */
public class ClauseModel {
    public static void main(String[] args) {
        EPServiceProvider provider = EPServiceProviderManager.getDefaultProvider();
//        segment515(provider.getEPAdministrator(),provider.getEPRuntime());
//        segment517(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment5271(provider.getEPAdministrator(),provider.getEPRuntime(), esper54.domain.Process.class.getName());
//        segment529(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment535(provider.getEPAdministrator(), provider.getEPRuntime(), StockTick.class.getName());
//        segment536(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment539(provider.getEPAdministrator(), provider.getEPRuntime(), StockTick.class.getName());
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
        Process event0 = new Process();
        event0.setIsLongRunning(false);
        event0.setProcessId(0);
        event0.setProcessName("maokitty0");
        runtime.sendEvent(event0);
        Process event1 = new Process();
        event1.setIsLongRunning(true);
        event1.setProcessId(1);
        event1.setProcessName("maokitty1");
        runtime.sendEvent(event1);
        Process event2 = new Process();
        event2.setIsLongRunning(false);
        event2.setProcessId(2);
        event2.setProcessName("maokitty2");
        runtime.sendEvent(event2);
        Process event3 = new Process();
        event3.setIsLongRunning(true);
        event3.setProcessId(3);
        event3.setProcessName("maokitty3");
        runtime.sendEvent(event3);
        Process event4 = new Process();
        event4.setIsLongRunning(true);
        event4.setProcessId(4);
        event4.setProcessName("maokitty4");
        runtime.sendEvent(event4);
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
        TimeUtil.sleepSec(5);
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
        TimeUtil.sleepSec(5);
        News news2 = new News();
        news2.setSymbol("maokitty0");
        news2.setText("maokitty05s后的一条新闻");
        runtime.sendEvent(news2);
//        runtime.sendEvent(tick0);//观察时间窗口长度不一致
        TimeUtil.sleepSec(5);
    }


    /**
     * EPL中schema和event type具有相同的含义
     * schema声明的statement如果不具备引用，那么引擎会自动删除这种事件类型
     * 创建schema默认使用的是map形式，即send对象的时候要使用 sendEvent(map,"schemaName")
     * @param admin
     * @param runtime
     */
    public static void segment515(EPAdministrator admin,EPRuntime runtime){
        String securityEvent = "create objectarray schema SecurityEvent as (ipAddress string , userId String,numAttempts int)";
        admin.createEPL(securityEvent);
        String schemaEpl = "select ipAddress , userId,numAttempts from SecurityEvent.win:time(4 sec)";
        EPStatement statement = admin.createEPL(schemaEpl);
        statement.addListener(new CommonListener());
        Object[] securityEventObj = new Object[]{"10.2.3.456","maokitty","1"};
        runtime.sendEvent(securityEventObj, "SecurityEvent");
    }

    /**
     * 变量在使用前需要声明或者配置
     * context partiton中访问变量的原子性和一致性由由engin来提供软保证(soft guarantee：任何一个statement,执行时间大于默认时间间隔(15s)，那么变量生效的时间是在15秒之后[经过测试在第一次调用之后，就生效了])
     * todo 和测试结果不一样
     *  If any of your application statements, in response to an event or timer invocation, execute for a time interval longer then 15 seconds (default interval length), then the engine may use current variable values after 15 seconds passed,
     * 变量接受null值，object代表任何类型，除了event_type_name，其它的都可以使用数组的形式
     * 通过on set语法改变变量的值，所有的statement立即可见
     * @param admin
     * @param runtime
     */
    public static void segment517(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema TickEvent as (symbol string,price double)");
        admin.createEPL("create variable integer var_output_rate = 16");
        admin.createEPL("select count(*) from TickEvent output every var_output_rate seconds").addListener(new CommonListener());
        admin.createEPL("create schema UpdateEvent()");
        admin.createEPL("on UpdateEvent set var_output_rate = 3");
        Map<String,Object> u0=MapSchemaEvent.getUpdateEvent();
        Map<String,Object> t0 = MapSchemaEvent.getTickEvent("maokitty1", 1.0);
        runtime.sendEvent(t0, "TickEvent");
        runtime.sendEvent(u0, "UpdateEvent");
        TimeUtil.sleepSec(21);

    }

    /**
     * =>和->等效，表达式左边是参数，右边是表达式
     * 表达式统计的是每次进来的事件
     * @param admin
     * @param runtime
     */
    public static void segment529(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema MarketDataEvent (buy double,sell double)");
        admin.createEPL("expression midPrice { x->(buy+sell)/2 } select midPrice(*) from MarketDataEvent").addListener(new CommonListener());
        Map<String,Object> obj0= MapSchemaEvent.getMarketDataEvent(1.0, 2.0);
        Map<String,Object> obj1=MapSchemaEvent.getMarketDataEvent(3.0, 2.0);
        runtime.sendEvent(obj0, "MarketDataEvent");
        runtime.sendEvent(obj1, "MarketDataEvent");
    }

    /**
     * 形如tick=StockTick -> news=News(symbol=tick.symbol)是一个lamada表达式
     * todo 语句select tick,news from StockTickAndNews和语句select * from StockTickAndNews输出结果不一样
     * @param admin
     * @param runtime
     */
    public static void segment536(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema News (symbol string,text string)");
        admin.createEPL("create schema StockTick(symbol string,price double)");
        admin.createEPL("create window StockTickAndNews.win:time(3 sec) (tick StockTick,news News)");
        admin.createEPL("insert into StockTickAndNews select tick,news from pattern[every tick=StockTick -> news=News(symbol=tick.symbol)]");
        admin.createEPL("select tick,news from StockTickAndNews").addListener(new SchemaCommonListener());
        Map<String,Object> obj00=MapSchemaEvent.getTickEvent("maokitty", 12);
        Map<String,Object> obj01=MapSchemaEvent.getNewsEvent("maokitty", "my text");
        Map<String,Object> obj10=MapSchemaEvent.getTickEvent("maokitty", 13);
        Map<String,Object> obj11=MapSchemaEvent.getNewsEvent("maokitty2", "my text 13");
        runtime.sendEvent(obj00,"StockTick");
        runtime.sendEvent(obj01,"News");
        runtime.sendEvent(obj10,"StockTick");
        runtime.sendEvent(obj11,"News");
    }
}
