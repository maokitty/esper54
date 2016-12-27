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
}
