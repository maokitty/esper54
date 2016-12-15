package esper54.chapter6;

import com.espertech.esper.client.*;
import esper54.Util.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by liwangchun on 16/11/15.
 * window创建本身必须要带有一个长度
 */
public class windowModel {
    private static Random random = new Random();
    public static void main(String[] args) {
        EPServiceProvider provider = EPServiceProviderManager.getDefaultProvider();
//        segment613(provider.getEPAdministrator(),provider.getEPRuntime());
        segment6212(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment66(provider.getEPAdministrator(), provider.getEPRuntime());
//        segment68(provider.getEPAdministrator(), provider.getEPRuntime());

    }


    public static void segment68(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema ProductTotalRec as (productId string,totalPrice double)");
        //直接使用事件类型创建window
        admin.createEPL("create window ProductWindow.std:unique(productId) as ProductTotalRec");
        admin.createEPL("select * from ProductWindow").addListener(new WindowCommonListener());
        admin.createEPL("create schema OrderEvent as (orderId string,productId string,price double,quantity int, deletedFlag boolean)");
        EPStatement mergeStatement=admin.createEPL("on OrderEvent oe merge ProductWindow pw where pw.productId = oe.productId " +
                "when matched then update set totalPrice=totalPrice+oe.price " +
                "when not matched then insert select productId,price as totalPrice");
        mergeStatement.addListener(new MergeCommonListener());
        Map<String,Object> orderSchema1 = getOrderSchema("1","1",10.01,1,false);
        Map<String,Object> orderSchema2 = getOrderSchema("2","1",10.01,1,false);
        Map<String,Object> orderSchema3 = getOrderSchema("3","2",10.01,1,false);
        runtime.sendEvent(orderSchema1,"OrderEvent");
        runtime.sendEvent(orderSchema2,"OrderEvent");
        runtime.sendEvent(orderSchema3,"OrderEvent");
    }
    /**
     * 条件不满足不会触发on update事件
     * 条件满足触发on update事件，会比windownListener先执行
     * @param admin
     * @param runtime
     */
    public static void segment66(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create window OrdersNamedWindow.win:time(30 sec)(orderId long,cosumerName string,volume long,price double,symbol string)");
        admin.createEPL("insert into OrdersNamedWindow select orderId,cosumerName,volume,price,symbol from `esper54.chapter6.Order`");
        EPStatement statementWin=admin.createEPL("select * from OrdersNamedWindow");
        statementWin.addListener(new WindowCommonListener());
        String opUpdateEpl = "on `esper54.chapter6.Order`(volume>0) as myNewOrders " +
                    "update OrdersNamedWindow as myNamedWindow " +
                    "set price = myNewOrders.price " +
                    "where myNamedWindow.symbol = myNewOrders.symbol";
        EPStatement statement = admin.createEPL(opUpdateEpl);
        statement.addListener(new UpdateCommonListener());
        Order order0=getOrder("maokitty", 1, 20, "mao", 1);
        runtime.sendEvent(order0);
        Order order1=getOrder("maokitty1",2,20,"mao",-1);
        runtime.sendEvent(order1);
        TimeUtil.sleepSec(1);
        Order order2=getOrder("maokitty1",3,200,"mao",-1);
        runtime.sendEvent(order2);
        Order order3=getOrder("maokitty1",3,200,"mao",1);
        runtime.sendEvent(order3);
    }

    /**
     * 窗口中使用schema当做eventtype,在插入数据的时候，新建对象使用new{}
     * todo 语句insert into SecurityEvent select new{name=datas.name,roles=datas.roles} from SecurityData as datas 和
     *      insert into SecurityEvent select new{name=datas.name,roles=datas.roles} as secData from SecurityData as datas 输出不一样
     */
    public static void segment6212(EPAdministrator admin,EPRuntime runtime){
        String securityDataSchema = "create objectarray schema SecurityData(name string,roles string[])";
        admin.createEPL(securityDataSchema);
//        推测包含实际应用场景
//        String securityEventWindow = "@EventRepresentation(array=true) create window SecurityEvent.win:time(30 sec) (ipAddress string,userId String,secData SecurityData,historySecData SecurityData[])";
        String securityEventWindow = "@EventRepresentation(array=true) create window SecurityEvent.win:time(30 sec) (secData SecurityData)";
        admin.createEPL(securityEventWindow);
//        String selectWindowEpl="select irstream ipAddress,userId,secData,historySecData from SecurityEvent";
        String selectWindowEpl="select irstream secData from SecurityEvent";
        admin.createEPL(selectWindowEpl).addListener(new SchemaCommonListener());;
        admin.createEPL("insert into SecurityEvent select new{name=datas.name,roles=datas.roles} from SecurityData as datas");
        Object[] securityData0=new Object[]{"maokitty0",new String[]{"user"}};
        Object[] securityData1=new Object[]{"maokitty1",new String[]{"user","admin"}};
        runtime.sendEvent(securityData0, "SecurityData");
        runtime.sendEvent(securityData1, "SecurityData");

    }
//    private

    /**
     * window
     * 1：须自己将事件insert进窗口
     * 2：window监听必须有事件触发
     * 3：window会存储他过滤条件里面的事件，通过查询语句可查出
     * 4：同时监听 事件和window会优先触发事件监听
     * @param admin
     * @param runtime
     */
    public static void segment613(EPAdministrator admin,EPRuntime runtime){
        String stockTickEvent = StockTick.class.getName();
        StringBuilder pBuilder = new StringBuilder("select irstream symbol,price from ");
        pBuilder.append(stockTickEvent);
        pBuilder.append(".win:time(1 sec)");
        String processEvent = pBuilder.toString();
        EPStatement statement = admin.createEPL(processEvent);
        statement.addListener(new CommonListener());
        StringBuilder windownBuilder = new StringBuilder("create window TenSecOfTicksWindow.win:time(10 sec) as ");
        windownBuilder.append(stockTickEvent);
        String window = windownBuilder.toString();
        StringBuilder insertWindowBuilder = new StringBuilder("insert into TenSecOfTicksWindow ");
        insertWindowBuilder.append(processEvent);
        String insertWindow = insertWindowBuilder.toString();
        admin.createEPL(window);
        admin.createEPL(insertWindow);
        StockTick tick0 = getStockTick();
        runtime.sendEvent(tick0);
        TimeUtil.sleepSec(2);
        StockTick tick1 = getStockTick();
        runtime.sendEvent(tick1);
        TimeUtil.sleepSec(2);
        System.out.println("stockTickEvent will be out while TenSecOfTicksWindow still hold this");
        String selectWindow = "select count(*) from TenSecOfTicksWindow";
        EPStatement windownStatement = admin.createEPL(selectWindow);
        windownStatement.addListener(new WindowCommonListener());
        //将processEvent移到这里观察先监听event还是先监听Window的影响
        runtime.sendEvent(tick0);
    }


    private static StockTick getStockTick(){
        StockTick tick = new StockTick();
        double price=random.nextDouble();
        tick.setPrice(price);
        tick.setSymbol("maokitty"+price);
        return tick;
    }
    private static Order getOrder(String cosumerName,long orderId,double price,String symbol,long volume){
        Order order=new Order();
        order.setCosumerName(cosumerName);
        order.setOrderId(orderId);
        order.setPrice(price);
        order.setSymbol(symbol);
        order.setVolume(volume);
        return order;
    }
    private static Map<String,Object> getOrderSchema(String orderId,String productId,double price,int quanlitity,boolean deleteFlag){
        Map<String,Object> schema = new HashMap<String, Object>();
        schema.put("orderId", orderId);
        schema.put("productId", productId);
        schema.put("price", price);
        schema.put("quantity", quanlitity);
        schema.put("deletedFlag", deleteFlag);
        return schema;
    }

}
