package esper54.chapter15;

import com.espertech.esper.client.*;
import com.espertech.esper.client.time.CurrentTimeEvent;
import com.espertech.esper.core.service.EPServiceProviderSPI;
import com.espertech.esper.core.thread.ThreadingService;
import esper54.Util.CommonListener;
import esper54.Util.MapSchemaEvent;
import esper54.Util.PatternCommonListener;
import esper54.Util.TimeUtil;
import esper54.domain.Order;
import esper54.domain.OrderHistory;
import esper54.subscriber.DefaultOrderEvent;
import esper54.subscriber.MultiOrderEvent;
import esper54.subscriber.OrderEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by liwangchun on 16/12/6.
 */
public class APIReference {
    public static void main(String[] args) {
//        EPServiceProvider provider = EPServiceProviderManager.getDefaultProvider();
//        segment155(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment1535(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment15715(provider);
//        segment15331(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment153311(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment153312(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment153314(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment153321(provider.getEPAdministrator(), provider.getEPRuntime());
        segment15101();
    }

    /**
     * on-demand-query
     * @param admin
     * @param runtime
     */
    public static void segment155(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema OrderEvent as(orderId string,price double)");
        admin.createEPL("create window MyNameWindow.win:time(10) as OrderEvent");
        admin.createEPL("insert into MyNameWindow select * from OrderEvent");
        Map<String,Object> order0= MapSchemaEvent.getOrderEvent("1", 1.01);
        Map<String,Object> order1=MapSchemaEvent.getOrderEvent("2", 2);
        Map<String,Object> order2=MapSchemaEvent.getOrderEvent("3", 2);
        runtime.sendEvent(order0,"OrderEvent");
        runtime.sendEvent(order1,"OrderEvent");
        runtime.sendEvent(order2,"OrderEvent");
        String query = "select * from MyNameWindow where orderId in(?) and price > ?";
        EPOnDemandPreparedQueryParameterized prepared = runtime.prepareQueryWithParameters(query);
        prepared.setObject(1,new String[]{"1","2"});
        prepared.setObject(2, 1.5);
        EPOnDemandQueryResult result=runtime.executeQuery(prepared);
        for (EventBean row:result.getArray()){
            System.out.println(row.getUnderlying());
        }
    }

    /**
     * subscriber相对listener的优势:
     * 1:性能高，查询结果直接传送给订阅的方法，不需要中间对象
     * 2:订阅者接收强类型的参数，订阅者的代码更简单
     * note 自定义的方法参数顺序和类型必须和select语句中的保持一致【遵循java标准】
     * @param admin
     * @param runtime
     */
    public static void segment15331(EPAdministrator admin,EPRuntime runtime){
        EPStatement statement=admin.createEPL("select orderId,price ,count(*) from `esper54.domain.Order`");
        statement.setSubscriber(new OrderEvent(),"orderIn");
        runtime.sendEvent(new Order(1,"maokitty",1,2.0,"mao"));
    }

    public static void segment153311(EPAdministrator admin,EPRuntime runtime){
        EPStatement statement=admin.createEPL("select *,count(*) from `esper54.domain.Order`.win:time(1 sec) o,`esper54.domain.OrderHistory`.win:time(1 sec) ");
        statement.setSubscriber(new OrderEvent(),"orderAndHistoryJoin");
        runtime.sendEvent(new Order(1, "maokitty", 1, 2.0, "mao"));
        runtime.sendEvent(new OrderHistory("maokittyHistory", 1));
        runtime.sendEvent(new OrderHistory("maokittyHistory2",2));
    }

    public static void segment153312(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema orderEvent(orderId long)");
        EPStatement statement=admin.createEPL("select *  from  orderEvent");
        statement.setSubscriber(new OrderEvent(), "orderMap");
        Map map = new HashMap();
        map.put("orderId", 1);
        runtime.sendEvent(map,"orderEvent");
    }

    public static void segment153314(EPAdministrator admin,EPRuntime runtime){
        EPStatement statement=admin.createEPL("select irstream orderId,count(*)  from  `esper54.domain.Order`.win:time(1 sec)");
        statement.setSubscriber(new DefaultOrderEvent());
        runtime.sendEvent(new Order(1, "maokitty", 1, 2.0, "mao"));
        TimeUtil.sleepSec(2);
    }

    /**
     * 每次有事件进来的时候触发一次
     * @param admin
     * @param runtime
     */
    public static void segment153321(EPAdministrator admin,EPRuntime runtime){
        EPStatement statement=admin.createEPL("select irstream *  from  `esper54.domain.Order`.win:time(1 sec)");
        statement.setSubscriber(new MultiOrderEvent());
        runtime.sendEvent(new Order(1, "maokitty", 1, 2.0, "mao"));
        runtime.sendEvent(new Order(2, "maokitty2", 2, 4.0, "mao2"));
        runtime.sendEvent(new Order(3, "maokitty3", 6, 6.0, "mao3"));
        TimeUtil.sleepSec(2);
    }

    /**
     * PULL API:EPStatement通过safeIterator和iterator获取数据[根据代码注释不同的部分来看]
     * safeIterator：线程安全,通过每个context partition加读写锁实现，同时给一个iteration一个读锁，使用后确保使用close方法
     * iterator：非线程安全
     *<p>safeIterator和iterator会立马返回结果，如果有output语句，对于没有使用group by或者aggregation，会立即输出
     *  有默认返回最后一次output group by或者aggregation的结果，想要实现类似的out put功能可用insert into来控制输出</p>
     * <p>statements不使用order by 语法，iterator会保持window里面的顺序</p>
     * <p>on-select场景需要trigger event来获取set集合或者通过下标获取iteration</p>
     * <p>使用iteration操作没有边界的数据流(未声明data window)：在有group by/aggregates 同时包括 output的情况下，engine会保留下他们的output来作为结果
     *    ;有group by/aggregates 没有output那么只使用最后的aggregation和最近跟新的group
     * </p>
     *
     *
     * @param admin
     * @param runtime
     */
    public static void segment1535(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema MyTick (price double)");
        EPStatement statement = admin.createEPL("select avg(price) as avgPrice from MyTick");
        Map<String,Object> tick1=getMyTick(2);
        Map<String,Object> tick2=getMyTick(3);
        runtime.sendEvent(tick1,"MyTick");
        runtime.sendEvent(tick2,"MyTick");
        //safe part
//        SafeIterator<EventBean> safeIter = statement.safeIterator();
//        try {
//            for (;safeIter.hasNext();) {
//                EventBean event = safeIter.next();
//                System.out.println("avg:" + event.get("avgPrice"));
//            } }
//        finally {
//            safeIter.close();     // Note: safe iterators must be closed
//        }
        //unsafe part
        double averagePrice = (Double) statement.iterator().next().get("avgPrice");
        System.out.println("avg:"+averagePrice);
    }

    /**
     * 获取默认的线程处理对列
     * todo 返回结果都是null
     * @param epService
     */
    public static void segment15715(EPServiceProvider epService){
        try{
            EPServiceProviderSPI spi = (EPServiceProviderSPI) epService;
            ThreadingService threadingService=spi.getThreadingService();
            BlockingQueue inQueue=threadingService.getInboundQueue();
            BlockingQueue outQueue=threadingService.getOutboundQueue();
            BlockingQueue timeQueue=threadingService.getTimerQueue();
            BlockingQueue routeQueue=threadingService.getRouteQueue();
            ThreadPoolExecutor threadpool = threadingService.getInboundThreadPool();
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     *
     */
    public static void segment15101(){
        Configuration configuration = new Configuration();
        configuration.getEngineDefaults().getViewResources().setShareViews(false);
        configuration.getEngineDefaults().getExecution().setAllowIsolatedService(true);
        //todo 使用默认的provider会出错，uri有啥用？先解决这个
//        EPServiceProvider provider=EPServiceProviderManager.getProvider("uri",configuration);
        EPServiceProvider provider=EPServiceProviderManager.getDefaultProvider(configuration);
        EPServiceProviderIsolated isolatedService = provider.getEPServiceIsolated("myIsolate");
        long startInMillis = System.currentTimeMillis();
        isolatedService.getEPRuntime().sendEvent(new CurrentTimeEvent(startInMillis));
        EPAdministrator admin=provider.getEPAdministrator();
        EPRuntime runtime=provider.getEPRuntime();
        admin.createEPL("create schema OrderEvent as(orderId string,price double)");
        EPStatement notIsolateStatement=admin.createEPL("select * from OrderEvent", "notIsolateStatementName");
        notIsolateStatement.addListener(new CommonListener());
        EPStatement toIsolataStatement = admin.getStatement("notIsolateStatementName");
        toIsolataStatement.addListener(new PatternCommonListener());
        isolatedService.getEPAdministrator().addStatement(toIsolataStatement);
        Map<String,Object> order0= MapSchemaEvent.getOrderEvent("1", 1.01);
        Map<String,Object> order1=MapSchemaEvent.getOrderEvent("2", 2);
        Map<String,Object> order2=MapSchemaEvent.getOrderEvent("3", 2);
        runtime.sendEvent(order0,"OrderEvent");
        runtime.sendEvent(order1, "OrderEvent");
        runtime.sendEvent(order2, "OrderEvent");
    }


    private static Map<String,Object> getMyTick(double price){
        Map<String,Object> tick = new HashMap<String, Object>();
        tick.put("price",price);
        return tick;
    }
}
