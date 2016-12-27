package esper54.chapter13;

import com.espertech.esper.client.*;
import esper54.Util.CommonListener;
import esper54.Util.MapSchemaEvent;
import esper54.Util.PatternCommonListener;
import esper54.Util.TimeUtil;

import java.awt.*;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by liwangchun on 16/12/23.
 *
 *  std: standard
 */
public class ViewsModel {
    public static void main(String[] args) {
        EPServiceProvider provider = EPServiceProviderManager.getDefaultProvider();
//        segment1341(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment1342(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment1343(provider.getEPAdministrator(), provider.getEPRuntime());
        segment1344(provider.getEPAdministrator(), provider.getEPRuntime());
    }

    /**
     * std:unique如果传送的是对象，需要自己实现hashCode和equals方法
     * select irstream * from StockTickEvent.std:unique(symbol) 含义
     * view里面只保留相同的新的symbol,旧的symbol会送给rstream
     * @Hint('disable_unique_implicit_idx') 迫使engine用non-unique index
     * @param admin
     * @param runtime
     */
    public static void segment1341(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema StockTickEvent (symbol string,price double,feed double)");
        admin.createEPL("select irstream * from StockTickEvent.std:unique(symbol)").addListener(new CommonListener());
        Map<String,Object> a1= MapSchemaEvent.getStockTick("maokitty1",1,2);
        Map<String,Object> a2= MapSchemaEvent.getStockTick("maokitty1", 10, 2);
        Map<String,Object> a3= MapSchemaEvent.getStockTick("maokitty2", 1.1, 2.0);
        Map<String,Object> a4= MapSchemaEvent.getStockTick("maokitty2", 1.1, 3.0);
        runtime.sendEvent(a1,"StockTickEvent");
        runtime.sendEvent(a2,"StockTickEvent");
        runtime.sendEvent(a3, "StockTickEvent");
        runtime.sendEvent(a4, "StockTickEvent");
        TimeUtil.sleepSec(3);
    }

    /**
     * groupwin 会为每一个view创建子view（group key不能用没有限制的key比如时间）
     * 如果传送的是对象，需要自己实现hashCode和equals方法
     * std:groupwin(symbol).win:length(1)和std:unique(symbol) 等效
     * length表示存放几个相同的key[单纯做聚合使用group by更好，如要控制数量则使用这个]
     * @Hint("reclaim_group_aged=age_in_seconds") 抛弃age_in_seconds后的数据【回收】
     * @Hint("reclaim_group_freq=sweep_frequency_in_seconds") 固定某个事件做一次清除，默认和age_in_seconds一样
     * 如果两个注解都没有不会执行这样的操作
     * todo 注解和表现不符合
     * 在使用其它view之前使用groupwin,其它的view只会对其子view产生影响
     * @note 在groupwin之后使用时间view和只使用时间view效果一样
     * @param admin
     * @param runtime
     */
    public static void segment1342(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema StockTickEvent (symbol string,price double,feed double)");
        admin.createEPL("@Hint('reclaim_group_aged=1,reclaim_group_freq=2') select irstream * from StockTickEvent.std:groupwin(symbol).win:length(3)").addListener(new CommonListener());
        Map<String,Object> a1= MapSchemaEvent.getStockTick("maokitty1",1,2);
        Map<String,Object> a2= MapSchemaEvent.getStockTick("maokitty2", 10, 3);
        Map<String,Object> a3= MapSchemaEvent.getStockTick("maokitty3", 10, 4);
        Map<String,Object> a4= MapSchemaEvent.getStockTick("maokitty4", 10, 5);
        Map<String,Object> a5= MapSchemaEvent.getStockTick("maokitty5", 10, 6);
        Map<String,Object> a6= MapSchemaEvent.getStockTick("maokitty6", 10, 6);
        Map<String,Object> a7= MapSchemaEvent.getStockTick("maokitty7", 10, 6);
        Map<String,Object> a8= MapSchemaEvent.getStockTick("maokitty8", 10, 6);
        Map<String,Object> a9= MapSchemaEvent.getStockTick("maokitty9", 10, 6);
        runtime.sendEvent(a1,"StockTickEvent");
        runtime.sendEvent(a2, "StockTickEvent");
        runtime.sendEvent(a3, "StockTickEvent");
        runtime.sendEvent(a4, "StockTickEvent");
        TimeUtil.sleepSec(2);
        runtime.sendEvent(a5, "StockTickEvent");
        runtime.sendEvent(a6, "StockTickEvent");
        runtime.sendEvent(a7, "StockTickEvent");
        runtime.sendEvent(a8, "StockTickEvent");
        runtime.sendEvent(a9, "StockTickEvent");
//        TimeUtil.sleepSec(1);
//        runtime.sendEvent(a4, "StockTickEvent");
        TimeUtil.sleepSec(2);
        TimeUtil.sleepSec(2);
    }

    /**
     * @param admin
     * @param runtime
     */
    public static void segment1343(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema StockTickEvent (symbol string,price double,feed double)");
        EPStatement statement=admin.createEPL("select size from StockTickEvent.win:time(1 sec).std:size()");
        statement.addListener(new CommonListener());
        Map<String,Object> a1= MapSchemaEvent.getStockTick("maokitty1",1,2);
        Map<String,Object> a2= MapSchemaEvent.getStockTick("maokitty2", 10, 3);
        Map<String,Object> a3= MapSchemaEvent.getStockTick("maokitty3", 10, 4);
        Map<String,Object> a4= MapSchemaEvent.getStockTick("maokitty4", 10, 5);
        runtime.sendEvent(a1,"StockTickEvent");
        runtime.sendEvent(a2, "StockTickEvent");
        runtime.sendEvent(a3, "StockTickEvent");
        runtime.sendEvent(a4, "StockTickEvent");
        Iterator<EventBean>  eventBeans=statement.iterator();
        if (eventBeans.hasNext()){
            EventBean eventBean=eventBeans.next();
            System.out.println(eventBean.getUnderlying()+" iterator");
        }
        TimeUtil.sleepSec(3);

    }

    /**
     * firstevent 只保留第一个事件，其它的事件都抛弃，如果使用了on-delete，那么会保留下一个到达的事件
     * lastevent 输出父窗口中的最后一个事件
     * std:firstunique 只保留第一个有同样值的事件 ，如果使用了on-delete，那么会保留下一个到达的事件
     *
     * @param admin
     * @param runtime
     */
    public static void segment1344(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema StockTickEvent (symbol string,price double,feed double)");
//        admin.createEPL("select irstream * from StockTickEvent.std:firstevent()").addListener(new CommonListener());
//        admin.createEPL("select irstream * from StockTickEvent(symbol='maokitty2').std:lastevent()").addListener(new CommonListener());
        admin.createEPL("select irstream * from StockTickEvent.std:firstunique(symbol)").addListener(new CommonListener());
        Map<String,Object> a1= MapSchemaEvent.getStockTick("maokitty1",1,2);
        Map<String,Object> a2= MapSchemaEvent.getStockTick("maokitty2", 10, 3);
        Map<String,Object> a3= MapSchemaEvent.getStockTick("maokitty2", 10, 4);
        Map<String,Object> a4= MapSchemaEvent.getStockTick("maokitty4", 10, 5);
        runtime.sendEvent(a1,"StockTickEvent");
        runtime.sendEvent(a2, "StockTickEvent");
        runtime.sendEvent(a3, "StockTickEvent");
        runtime.sendEvent(a4, "StockTickEvent");
    }


}
