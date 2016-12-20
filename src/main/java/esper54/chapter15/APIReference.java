package esper54.chapter15;

import com.espertech.esper.client.*;
import com.espertech.esper.core.service.EPServiceProviderSPI;
import com.espertech.esper.core.thread.ThreadingService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by liwangchun on 16/12/6.
 */
public class APIReference {
    public static void main(String[] args) {
        EPServiceProvider provider = EPServiceProviderManager.getDefaultProvider();
//        segment155(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment1535(provider.getEPAdministrator(), provider.getEPRuntime());
        segment15715(provider);
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
        Map<String,Object> order0=getOrderEvent("1",1.01);
        Map<String,Object> order1=getOrderEvent("2",2);
        Map<String,Object> order2=getOrderEvent("3",2);
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

    private static Map<String,Object> getOrderEvent(String orderId,double price){
        Map<String,Object> orderMap = new HashMap<String, Object>();
        orderMap.put("orderId",orderId);
        orderMap.put("price",price);
        return orderMap;
    }
    private static Map<String,Object> getMyTick(double price){
        Map<String,Object> tick = new HashMap<String, Object>();
        tick.put("price",price);
        return tick;
    }
}
