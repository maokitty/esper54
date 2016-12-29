package esper54.Util;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by liwangchun on 16/12/23.
 */
public class MapSchemaEvent {
    public static Map<String,Object> getStockTick(String symbol,double price,double feed){
        Map<String,Object> stockTicks=new HashMap<String, Object>();
        stockTicks.put("symbol",symbol);
        stockTicks.put("price",price);
        stockTicks.put("feed",feed);
        return stockTicks;
    }

    public static Map<String,Object> getOrderEvent(String orderId,double price){
        Map<String,Object> orderMap = new HashMap<String, Object>();
        orderMap.put("orderId",orderId);
        orderMap.put("price",price);
        return orderMap;
    }
    public static Map<String,Object> getTickEvent(String symbol,double price){
        Map<String,Object> tick = new HashMap<String, Object>();
        tick.put("symbol", symbol);
        tick.put("price", price);
        return tick;
    }
    public static Map<String,Object> getMarketDataEvent(double buy,double sell){
        Map<String,Object> obj = new HashMap<String, Object>();
        obj.put("buy",buy);
        obj.put("sell",sell);
        return obj;
    }
    public static Map<String,Object> getNewsEvent(String symbol,String text){
        Map<String,Object> obj = new HashMap<String, Object>();
        obj.put("symbol",symbol);
        obj.put("text",text);
        return obj;
    }

    public static Map<String,Object> getUpdateEvent(){
        Map<String,Object> update = new HashMap<String, Object>();
        return update;
    }
}
