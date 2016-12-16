package esper54.chapter7;

import com.espertech.esper.client.*;
import esper54.Util.CommonListener;
import esper54.Util.PatternCommonListener;
import esper54.Util.SchemaCommonListener;
import esper54.Util.TimeUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by liwangchun on 16/12/9.
 */
public class PatternModel {
    public static void main(String[] args) {
        EPServiceProvider provider = EPServiceProviderManager.getDefaultProvider();

//        segment711(provider.getEPAdministrator(),provider.getEPRuntime());
//        segment724(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment726(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment741(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment752(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment753(provider.getEPAdministrator(), provider.getEPRuntime());
        segment763(provider.getEPAdministrator(), provider.getEPRuntime());
    }

    /**
     * select中使用pattern,查询出来的字段是pattern中命名的
     * @param admin
     * @param runtime
     */
    public static void segment711(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema ServiceMeasurement (latency long,success boolean)");
        admin.createEPL("select * from pattern [ every (spike=ServiceMeasurement(latency>200) or error=ServiceMeasurement(success=false))]").addListener(new PatternCommonListener());
        Map<String,Object> sM0=getServiceMeasurementEvent(300, true);
        Map<String,Object> sM1=getServiceMeasurementEvent(200,true);
        Map<String,Object> sM2=getServiceMeasurementEvent(100, false);
        runtime.sendEvent(sM0,"ServiceMeasurement");
        runtime.sendEvent(sM1,"ServiceMeasurement");
        runtime.sendEvent(sM2, "ServiceMeasurement");
    }

    /**
     * @IterableUnbound 必须要添加在statement里面，没有无法拿到，并且只会返回最后一个触发pattern事件的值
     * @param admin
     * @param runtime
     */
    public static void segment724(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema ServiceMeasurement (latency long,success boolean)");
        EPStatement mytrigger=admin.createPattern("@IterableUnbound every (spike=ServiceMeasurement(latency>200) or error=ServiceMeasurement(success=false))");
        Map<String,Object> sM0=getServiceMeasurementEvent(300,true);
        Map<String,Object> sM1=getServiceMeasurementEvent(200, true);
        Map<String,Object> sM2=getServiceMeasurementEvent(100,false);
        runtime.sendEvent(sM0,"ServiceMeasurement");
        runtime.sendEvent(sM1,"ServiceMeasurement");
        runtime.sendEvent(sM2, "ServiceMeasurement");
        if (mytrigger.iterator().hasNext()){
            Map<String,Object> spike=(Map<String,Object>)mytrigger.iterator().next().get("spike");
            Map<String,Object> error=(Map<String,Object>)mytrigger.iterator().next().get("error");
            if(spike!=null){
                System.out.println(spike.toString());
            }
            if (error!=null){
                System.out.println(error.toString());
            }
        }else{
            System.out.println("no event");
        }

    }

    /**
     *重复时间:pattern every a=A->B[A，B事件都有,并且A事件先发生,”->“ followed by 的意思],事件序列是 A1,A2,B1;{A1,B1}和{A2,B1}都满足；
     * 加上注解@SuppressOverlappingMatches,engine探测到B1事件有重叠就不再输出
     * note engine只考虑加了tag的事件，join中不能使用
     * @param admin
     * @param runtime
     */
    public static void segment726(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema A (a string)");
        admin.createEPL("create schema B (b string)");
        admin.createEPL("select a,b from pattern @SuppressOverlappingMatches [every a=A->b=B]").addListener(new PatternCommonListener());
        Map<String,Object> a1=getAEvent("a1");
        Map<String,Object> a2 = getAEvent("a2");
        Map<String,Object> b1=getBEvent("b1");
        runtime.sendEvent(a1,"A");
        runtime.sendEvent(a2,"A");
        runtime.sendEvent(b1,"B");
    }

    /**
     * 到达的事件会被所有的有效的过滤器执行，添加 @consume 可以控制当事件被处理之后，不会再被其它的满足pattern的事件处理，对应的
     * 给consume添加level,level越大，越优先处理,默认取1，值相同的全部匹配
     * 优先级最好加上括号 every (a=RfidEvent(zone='Z1')@consume(2) or b=RfidEvent(assetId='0001')@consume(1) or c=RfidEvent(category='perishable'))
     * @param admin
     * @param runtime
     */
    public static void segment741(EPAdministrator admin,EPRuntime runtime){
         admin.createEPL("create schema RfidEvent(zone string,assetId string,category string)");
         admin.createEPL("select a,b,c from pattern [every (a=RfidEvent(zone='Z1')@consume(2) or b=RfidEvent(assetId='0001')@consume(1) or c=RfidEvent(category='perishable'))]").addListener(new PatternCommonListener());
        Map<String,Object> r1=getRfidEvent("Z1","BBB","CCC");
        Map<String,Object> r2=getRfidEvent("AAa", "0001", "ccc");
        Map<String,Object> r3=getRfidEvent("Aaa", "ddd", "perishable");
        Map<String,Object> r4=getRfidEvent("bb", "cc", "perishable");
        runtime.sendEvent(r1,"RfidEvent");
        runtime.sendEvent(r2,"RfidEvent");
        runtime.sendEvent(r3,"RfidEvent");
        runtime.sendEvent(r4,"RfidEvent");

    }

    /**
     * every-distinct 指定时间内去重
     * @param admin
     * @param runtime
     */
    public static void segment752(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema A (a string)");
        admin.createEPL("select a from pattern [every-distinct(a.a,3 seconds) a=A]").addListener(new PatternCommonListener());
        Map<String,Object> a1=getAEvent("a1");
        Map<String,Object> a2 = getAEvent("a2");
        runtime.sendEvent(a1,"A");
        runtime.sendEvent(a1,"A");
        runtime.sendEvent(a2,"A");
        TimeUtil.sleepSec(4);
        runtime.sendEvent(a1,"A");
    }

    /**
     * 事件重复一定次数才能触发
     * @param admin
     * @param runtime
     */
    public static void segment753(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema A (a string)");
        admin.createEPL("select a from pattern [ every [3] a=A]").addListener(new PatternCommonListener());
        Map<String,Object> a1=getAEvent("a1");
        Map<String,Object> a2 = getAEvent("a2");
        runtime.sendEvent(a1,"A");
        runtime.sendEvent(a1, "A");
        runtime.sendEvent(a2,"A");
    }

    /**
     *
     * @param admin
     * @param runtime
     */
    public static void segment763(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema A (a string)");
        admin.createEPL("select a from pattern [ every a=A->timer:interval(3 seconds)]").addListener(new PatternCommonListener());
        Map<String,Object> a1=getAEvent("a1");
        runtime.sendEvent(a1,"A");
        TimeUtil.sleepSec(3);
    }

    private static Map<String,Object> getServiceMeasurementEvent(long latency,boolean success){
        Map<String,Object> sM=new HashMap<String, Object>();
        sM.put("latency",latency);
        sM.put("success",success);
        return sM;
    }
    private  static Map<String,Object> getAEvent(String a){
        Map<String,Object> sM=new HashMap<String, Object>();
        sM.put("a",a);
        return sM;
    }
    private  static Map<String,Object> getBEvent(String b){
        Map<String,Object> sM=new HashMap<String, Object>();
        sM.put("b",b);
        return sM;
    }
    private  static Map<String,Object> getRfidEvent(String zone,String assetId,String category){
        Map<String,Object> sM=new HashMap<String, Object>();
        sM.put("zone",zone);
        sM.put("assetId",assetId);
        sM.put("category",category);
        return sM;
    }
}
