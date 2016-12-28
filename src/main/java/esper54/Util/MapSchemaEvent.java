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
}
