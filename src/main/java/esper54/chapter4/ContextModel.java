package esper54.chapter4;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import esper54.Util.CommonListener;
import esper54.Util.PatternCommonListener;
import esper54.Util.TimeUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by liwangchun on 16/12/15.
 * todo context partition是如何影响的
 */
public class ContextModel {
    public static void main(String[] args) {
        EPServiceProvider provider = EPServiceProviderManager.getDefaultProvider();
//        segment422(provider.getEPAdministrator(),provider.getEPRuntime());
//        segment423(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment424(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment425(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment426(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment4261(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment4272(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment43(provider.getEPAdministrator(), provider.getEPRuntime());
        segment45(provider.getEPAdministrator(), provider.getEPRuntime());
    }

    /**
     * custId不同不会触发
     * @param admin
     * @param runtime
     */
    public static void segment422(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema BankTxn (custId long,account string,amount double)");
        admin.createEPL("create context SegmentedByCustomer partition by custId from BankTxn");
        //where timer:within(3 seconds) 只匹配接下来的3秒，之后的不再输出
        admin.createEPL("context SegmentedByCustomer select a,b from pattern [every a=BankTxn(amount>400)->b=BankTxn(amount>400) where timer:within(3 seconds)]").addListener(new PatternCommonListener());
        Map<String,Object> o1=getBankTxn(1,"maokitty1",500);
        Map<String,Object> o2=getBankTxn(1, "maokitty2", 600);
        Map<String,Object> o3=getBankTxn(1, "maokitty3", 700);
        Map<String,Object> o4=getBankTxn(1, "maokitty3", 800);
        runtime.sendEvent(o1,"BankTxn");
        runtime.sendEvent(o2,"BankTxn");
        runtime.sendEvent(o3,"BankTxn");
        TimeUtil.sleepSec(4);
        runtime.sendEvent(o4, "BankTxn");
    }

    /**
     * create context SegmentedByCustomerHash coalesce by consistent_hash_crc32(custId) from BankTxn granularity 16 preallocate 含义
     * 使用hash算法 consistent_hash_crc32 最多创建16个context partition[每个statement最多分配16个线程并发处理]，statement一关联到context就创建所有的context partition
     * 这样有可能相同的id分配到同一个context里面，在查询的时候最好带上group by
     * note 使用preallocate时 granularity最好小于1k
     * @param admin
     * @param runtime
     */
    public static void segment423(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema BankTxn (custId long,account string,amount double)");
        admin.createEPL("create context SegmentedByCustomerHash coalesce by consistent_hash_crc32(custId) from BankTxn granularity 3 preallocate");
        admin.createEPL("context SegmentedByCustomerHash select custId, account, sum(amount) from BankTxn group by custId").addListener(new PatternCommonListener());
        Map<String,Object> o1=getBankTxn(1,"maokitty1",500);
        Map<String,Object> o2=getBankTxn(2,"maokitty2",600);
        Map<String,Object> o3=getBankTxn(3,"maokitty3",700);
        Map<String,Object> o4=getBankTxn(4,"maokitty4",800);
        runtime.sendEvent(o1, "BankTxn");
        runtime.sendEvent(o2, "BankTxn");
        runtime.sendEvent(o3, "BankTxn");
        runtime.sendEvent(o4, "BankTxn");
        runtime.sendEvent(o4, "BankTxn");
    }

    /**
     * LABEL 内置变量名，获取category被分配的名字
     * @param admin
     * @param runtime
     */
    public static void segment424(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema BankTxn (custId long,account string,amount double)");
        admin.createEPL("create context SegmentedByCustomerCategory group amount < 600 as low ,group amount between 600 and 700 as medium ,group amount > 700 as high from BankTxn");
        admin.createEPL("context SegmentedByCustomerCategory select context.label, count(*) from BankTxn").addListener(new PatternCommonListener());
        Map<String,Object> o1=getBankTxn(1,"maokitty1",500);
        Map<String,Object> o2=getBankTxn(2,"maokitty2",600);
        Map<String,Object> o3=getBankTxn(3,"maokitty3",700);
        Map<String,Object> o4=getBankTxn(4,"maokitty4",800);
        runtime.sendEvent(o1, "BankTxn");
        runtime.sendEvent(o2, "BankTxn");
        runtime.sendEvent(o3, "BankTxn");
        runtime.sendEvent(o4, "BankTxn");
    }

    /**
     * start条件满足时，engine不会再留意start,只有end条件满足的时候，才会重新看start条件
     * create context PowerOutage start powerout end pattern [poweron->timer:interval(2 seconds)] 含义
     * 搜集powerout事件到达的时候，到poweron 事件到达后两秒内的所有数据
     * @param admin
     * @param runtime
     */
    public static void segment425(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema TemperatureEvent (time string,temperature double)");
        admin.createEPL("create schema poweron()");
        admin.createEPL("create schema powerout()");
        admin.createEPL("create context PowerOutage start powerout end pattern [poweron->timer:interval(2 seconds)]" );
        admin.createEPL("context PowerOutage select * from TemperatureEvent").addListener(new CommonListener());
        Map<String,Object> o1=getTemperature("1",20);
        Map<String,Object> on=getPowerOnSingal();
        Map<String,Object> out=getPowerOutageSingal();
        Map<String,Object> o2=getTemperature("2", 30);
        Map<String,Object> o3=getTemperature("3", 40);
        Map<String,Object> o4=getTemperature("4", 50);
        Map<String,Object> o5=getTemperature("5", 60);
        runtime.sendEvent(o1, "TemperatureEvent");
        runtime.sendEvent(out, "powerout");
        runtime.sendEvent(o2, "TemperatureEvent");
        runtime.sendEvent(o3, "TemperatureEvent");
        runtime.sendEvent(on, "poweron");
        TimeUtil.sleepSec(3);
        runtime.sendEvent(o4, "TemperatureEvent");
        runtime.sendEvent(o5, "TemperatureEvent");
    }

    /**
     * create context CtxTrainEnter initiated by TrainEnterEvent as te terminated after 2 seconds 含义
     * TrainEnterEvent进入的时候分配partition,并在2秒之后结束
     * context CtxTrainEnter select t1 from pattern [t1=TrainEnterEvent->timer:interval(1 seconds) and not TrainLeavefEvent(id=Context.te.id)] 含义
     * CtxTrainEnter context中TrainEnterEvent到达后的1s之内没有发生TrainLeavefEvent
     *
     * @param admin
     * @param runtime
     */
    public static void segment426(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema TrainEnterEvent (id long,time string)");
        admin.createEPL("create schema TrainLeavefEvent() copyfrom TrainEnterEvent");
        admin.createEPL("create context CtxTrainEnter initiated by TrainEnterEvent as te terminated after 2 seconds");
        admin.createEPL("context CtxTrainEnter select t1 from pattern [t1=TrainEnterEvent->timer:interval(1 seconds) and not TrainLeavefEvent(id=context.te.id)]").addListener(new PatternCommonListener());
        Map<String,Object> o1=getTrainEvent(1,"1");
        Map<String,Object> o2=getTrainEvent(2,"2");
        runtime.sendEvent(o1, "TrainEnterEvent");
        runtime.sendEvent(o2, "TrainLeavefEvent");
        TimeUtil.sleepSec(3);
    }

    /**
     * create context CtxTrainEnter initiated by distinct(id) TrainEnterEvent as te terminated by TrainLeaveEvent(id=te.id)
     * TrainEnterEvent时开始创建context,TrainLeaveEvent结束，这个时候相同的id才会被新建context
     * @param admin
     * @param runtime
     */
    public static void segment4261(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema TrainEnterEvent (id long,time string)");
        admin.createEPL("create schema TrainLeaveEvent() copyfrom TrainEnterEvent");
        admin.createEPL("create context CtxTrainEnter initiated by distinct(id) TrainEnterEvent as te terminated by TrainLeaveEvent(id=te.id)");
        admin.createEPL("context CtxTrainEnter select t1 from pattern [t1=TrainEnterEvent->timer:interval(1 seconds)]").addListener(new PatternCommonListener());
        Map<String,Object> o1=getTrainEvent(1,"1");
        Map<String,Object> o2=getTrainEvent(1,"2");
        runtime.sendEvent(o1, "TrainEnterEvent");
        runtime.sendEvent(o2, "TrainLeaveEvent");
//        TimeUtil.sleepSec(3);
    }

    /**
     * @inclusive 没有加第一次不会匹配；加上了每次都会匹配
     * @param admin
     * @param runtime
     */
    public static void segment4272(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema TrainEnterEvent (id long,time string)");
        admin.createEPL("create schema TrainLeaveEvent() copyfrom TrainEnterEvent");
//        admin.createEPL("create context CtxTrainEnter start pattern [a=TrainEnterEvent or b=TrainLeaveEvent] @inclusive end after 3 seconds");
        admin.createEPL("create context CtxTrainEnter start pattern [TrainEnterEvent or TrainLeaveEvent] end after 3 seconds");
        admin.createEPL("context CtxTrainEnter select t1,t2 from pattern [every t1=TrainEnterEvent or every t2=TrainLeaveEvent]").addListener(new PatternCommonListener());
        Map<String,Object> o1=getTrainEvent(1,"1");
        Map<String,Object> o2=getTrainEvent(1,"2");
        Map<String,Object> o3=getTrainEvent(1,"2");
        runtime.sendEvent(o1, "TrainEnterEvent");
        runtime.sendEvent(o2, "TrainLeaveEvent");
        runtime.sendEvent(o3, "TrainEnterEvent");
    }

    /**
     * 嵌套context和顺序相关
     * create context CtxNestedTrainEnter context InitCtx initiated by TrainEnterEvent as te terminated after 2 seconds ,  context HashCtx coalesce by consistent_hash_crc32(tagId) from PassengerScanEvent   granularity 16 preallocate
     * 当TrainEnterEvent事件发生后才开始PassengerScanEvent，总共只计算TrainEnterEvent之后的2秒钟
     * @param admin
     * @param runtime
     */
    public static void segment43(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema TrainEnterEvent (id long,time string)");
        admin.createEPL("create schema PassengerScanEvent (tagId long)");
        admin.createEPL("create context CtxNestedTrainEnter context InitCtx initiated by TrainEnterEvent as te terminated after 2 seconds ,  context HashCtx coalesce by consistent_hash_crc32(tagId) from PassengerScanEvent   granularity 16 preallocate");
        admin.createEPL("context CtxNestedTrainEnter select context.InitCtx.te.id, context.HashCtx.id, tagId, count(*) from PassengerScanEvent group by tagId").addListener(new CommonListener());
        Map<String,Object> o1=getTrainEvent(1,"1");
        Map<String,Object> o2=getTrainEvent(2,"2");
        Map<String,Object> o3=getTrainEvent(3,"3");
        Map<String,Object> p1=getPassengerScan(1);
        Map<String,Object> p2=getPassengerScan(2);
        runtime.sendEvent(o1, "TrainEnterEvent");
        runtime.sendEvent(o2, "TrainEnterEvent");
        runtime.sendEvent(o3, "TrainEnterEvent");
        runtime.sendEvent(p1,"PassengerScanEvent");
        TimeUtil.sleepSec(3);
        runtime.sendEvent(p2,"PassengerScanEvent");
    }

    /**
     * create context CtxEachSec initiated by pattern [every timer:interval(1 sec)] terminated after 3 seconds
     * 每秒钟开始一次，3秒之后当前的context就过期，新建一个context,之前的数据都没了
     * @param admin
     * @param runtime
     */
    public static void segment45(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema SensorEvent (idS long,temp double)");
        admin.createEPL("create context CtxEachSec initiated by pattern [every timer:interval(1 sec)] terminated after 3 seconds");
        admin.createEPL("context CtxEachSec select context.id, avg(temp) from SensorEvent output snapshot when terminated").addListener(new CommonListener());
        Map<String,Object> o1=getSensor(1,20);
        Map<String,Object> o2=getSensor(2,30);
        Map<String,Object> o3=getSensor(3,40);
        TimeUtil.sleepSec(1);
        runtime.sendEvent(o1, "SensorEvent");
        runtime.sendEvent(o2, "SensorEvent");
        TimeUtil.sleepSec(3);
        runtime.sendEvent(o3, "SensorEvent");
        TimeUtil.sleepSec(9);
    }

    private static  Map<String,Object> getSensor(long id,double temp){
        Map<String,Object> obj=new HashMap<String, Object>();
        obj.put("idS", id);
        obj.put("temp", temp);
        return obj;
    }

    private static Map<String,Object> getBankTxn(long custId,String account,double amount){
        Map<String,Object> obj=new HashMap<String, Object>();
        obj.put("custId", custId);
        obj.put("account", account);
        obj.put("amount", amount);
        return obj;
    }
    private static Map<String,Object> getTrainEvent(long id,String time){
        Map<String,Object> obj=new HashMap<String, Object>();
        obj.put("id", id);
        obj.put("time", time);
        return obj;
    }
    private static Map<String,Object> getPassengerScan(long tagId){
        Map<String,Object> obj=new HashMap<String, Object>();
        obj.put("tagId", tagId);
        return obj;
    }

    private static Map<String,Object> getPowerOutageSingal(){
        return new HashMap<String, Object>();
    }
    private static Map<String,Object> getPowerOnSingal(){
        return new HashMap<String, Object>();
    }
    private static Map<String,Object> getTemperature(String time,double temperature){
        Map<String,Object> obj=new HashMap<String, Object>();
        obj.put("time",time);
        obj.put("temperature",temperature);
        return obj;
    }
}
