1:context将时间分成多个集合[context partition],每个context之间相互独立，有自己的声明周期。一个context可以被多个statement共享，如果statement没有声明
  context,esper会给它分配一个context paartition,起生命周期同statement一样
2:context创建不会处理任何逻辑，直到属于他的statemnt创建或者启动，如果所有的statments都被销毁了，那么context再度失效，直至下次激活。激活的
  context本身产生的statement不会有任何结果输出
3:一个context里面不能存在两个一样的事件流，如果事件属性不存在也是不能使用；声明多个流的同时，每个流的属性类型必须一致
4:context partition方式有如下两种：
  1：context自身属性;
  2：通过键来分片:
  3：hash算法分片:
  4：category分片:
  5：no-overlaping分片：
  6：overlaping分片
  7：根据条件分片
5:不使用context语法也可以达到分片的目的
6:通过output控制输出的时间
7:联合命名window或者table使用context
