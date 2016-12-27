package esper54.chapter16;

import com.espertech.esper.client.*;
import esper54.Util.TimeUtil;
import esper54.domain.Order;
import esper54.subscriber.MultiOrderEvent;

import java.util.Iterator;

/**
 * Created by liwangchun on 16/12/23.
 */
public class ConfigurationModel {
    public static void main(String[] args) {
        segment164132();
    }

    /**
     * 使用iteration获取没有边界的流的时候，没有返回值，通过增加如下配置可实现
     * config.getEngineDefaults().getViewResources().setIterableUnbound(true);
     */
    public static void segment164132(){
        Configuration config=new Configuration();
        config.getEngineDefaults().getViewResources().setIterableUnbound(true);
        EPServiceProvider provider=EPServiceProviderManager.getDefaultProvider(config);
//        EPServiceProvider provider=EPServiceProviderManager.getDefaultProvider(); //注意与上面的区别
        EPAdministrator admin=provider.getEPAdministrator();
        EPRuntime runtime=provider.getEPRuntime();
        EPStatement statement=admin.createEPL("select irstream *  from  `esper54.domain.Order`");
        runtime.sendEvent(new Order(1, "maokitty", 1, 2.0, "mao"));
        runtime.sendEvent(new Order(2, "maokitty2", 2, 4.0, "mao2"));
        runtime.sendEvent(new Order(3, "maokitty3", 6, 6.0, "mao3"));
        Iterator<EventBean> iterators =  statement.iterator();
        while(iterators.hasNext()){
            EventBean bean=iterators.next();
            System.out.println(bean.getUnderlying());
        }
    }
}
