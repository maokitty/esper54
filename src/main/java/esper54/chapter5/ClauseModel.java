package esper54.chapter5;

import com.espertech.esper.client.*;
import esper54.Util.CommonListener;
import esper54.Util.SchemaCommonListener;
import esper54.Util.TimeUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by liwangchun on 16/11/15.
 */
public class ClauseModel {
    public static void main(String[] args) {
        EPServiceProvider provider = EPServiceProviderManager.getDefaultProvider();
//        segment515(provider.getEPAdministrator(),provider.getEPRuntime());
//        segment517(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment529(provider.getEPAdministrator(), provider.getEPRuntime());
        segment536(provider.getEPAdministrator(), provider.getEPRuntime());
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
        Map<String,Object> u0=getUpdateEvent();
        Map<String,Object> t0 = getTickEvent("maokitty1", 1.0);
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
        Map<String,Object> obj0=getMarketDataEvent(1.0, 2.0);
        Map<String,Object> obj1=getMarketDataEvent(3.0, 2.0);
        runtime.sendEvent(obj0,"MarketDataEvent");
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
        Map<String,Object> obj00=getTickEvent("maokitty",12);
        Map<String,Object> obj01=getNewsEvent("maokitty","my text");
        Map<String,Object> obj10=getTickEvent("maokitty",13);
        Map<String,Object> obj11=getNewsEvent("maokitty2", "my text 13");
        runtime.sendEvent(obj00,"StockTick");
        runtime.sendEvent(obj01,"News");
        runtime.sendEvent(obj10,"StockTick");
        runtime.sendEvent(obj11,"News");
    }

    private static Map<String,Object> getTickEvent(String symbol,double price){
        Map<String,Object> tick = new HashMap<String, Object>();
        tick.put("symbol", symbol);
        tick.put("price", price);
        return tick;
    }
    private static Map<String,Object> getMarketDataEvent(double buy,double sell){
        Map<String,Object> obj = new HashMap<String, Object>();
        obj.put("buy",buy);
        obj.put("sell",sell);
        return obj;
    }
    private static Map<String,Object> getNewsEvent(String symbol,String text){
        Map<String,Object> obj = new HashMap<String, Object>();
        obj.put("symbol",symbol);
        obj.put("text",text);
        return obj;
    }

    private static Map<String,Object> getUpdateEvent(){
        Map<String,Object> update = new HashMap<String, Object>();
        return update;
    }
}
