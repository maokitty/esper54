package esper54.chapter16;

import com.espertech.esper.client.*;
import esper54.Util.CommonListener;
import esper54.Util.TimeUtil;
import esper54.domain.Order;
import esper54.subscriber.MultiOrderEvent;

import java.sql.Time;
import java.util.Iterator;

/**
 * Created by liwangchun on 16/12/23.
 * getDefault provider如果刚开始初始化的时候没有用config,后面再用config初始化也不会有用
 */
public class ConfigurationModel {
    public static void main(String[] args) {
//        segment164132();
        segment164133();
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

    /**
     * 默认情况下，esper中的多个窗口是当做交集处理，除非提供retain-union做并。
     * select irstream *  from  `esper54.domain.Order`.std:unique(price).std:unique(cosumerName) 含义
     *   默认price和cosumerName都必须是唯一的
     *select irstream *  from  `esper54.domain.Order`.std:unique(price).std:unique(cosumerName) retain-union 含义
     *   按照两个都一样才移除
     * AllowMultipleExpiryPolicies 设置成true,事件到达，最顶层的window接收插入事件,并依次将每一个插入事件传递到链条中的数据窗口
     * 每一个窗口可以按照自己的过期策略来移除事件，过期的事件只能传递到下层的数据窗口不能往前传递
     * 建议设置成fasle
     */
    public static void segment164133(){
//        Configuration config=new Configuration();
//        config.getEngineDefaults().getViewResources().setAllowMultipleExpiryPolicies(true);
//        EPServiceProvider provider=EPServiceProviderManager.getDefaultProvider(config);
        EPServiceProvider provider=EPServiceProviderManager.getDefaultProvider(); //注意与上面的区别
        EPAdministrator admin=provider.getEPAdministrator();
        EPRuntime runtime=provider.getEPRuntime();
        EPStatement statement=admin.createEPL("select irstream *  from  `esper54.domain.Order`.std:unique(price).std:unique(cosumerName) retain-union");
//        EPStatement statement=admin.createEPL("select irstream *  from  `esper54.domain.Order`.std:unique(price).std:unique(cosumerName)");
        statement.addListener(new CommonListener());
        runtime.sendEvent(new Order(1, "maokitty", 1, 2.0, "mao"));
        runtime.sendEvent(new Order(2, "maokitty2", 2, 3.0, "mao2"));
        runtime.sendEvent(new Order(3, "maokitty2", 6, 4.0, "mao3"));
        runtime.sendEvent(new Order(3, "maokitty4", 6, 5.0, "mao3"));
        TimeUtil.sleepSec(2);
    }
}
