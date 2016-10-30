官方文档：
http://www.espertech.com/esper/release-5.4.0/esper-reference/html/processingmodel.html

1:esper处理事件有两种方式：
 A实现接口 com.espertech.esper.client.UpdateListener;
 B:订阅statement,直接将查询结果绑定到java对象，对象本身不需要实现任何接口和父类,性能更高;

2:实现接口事件处理结果放在 com.espertech.esper.client.EventBean{get,getUnderlying,getEventType};
get 方法可获取epl语句中定义的属性，可以是传入属性或者属性的表达式
getUnderlying 获取正在处理事件的类型，如果select用的是通配符，那么获取的类型就是通过sendEvent传入的对象，
              对于joins或者select语句中包含表达式，那么返回的是java.util.map
getEventType

3: