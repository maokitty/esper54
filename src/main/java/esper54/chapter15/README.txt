1：EPServiceProvider之间互相独立,因此两个引擎之间无法互相送信息
2：EPServiceProviderManager用来获取EPServiceProvider，通过getProvider方法提供不同的URI可获取不同esper引擎，URI存在则返回同一个实例
  否则再创建一个
3: EPServiceProvider的initialize方法可以用来重置自己，所有之前的设置都无效，需要重新获取
4: EPServiceProvider的destroy方法会将所有的staments都废弃，并释放所有的资源
5：EPServiceStateListener可用来监听initialize和destroy,通过addServiceStateListener来注册
6: EPStatementStateListener可用来监听创建新的statements，以及stement的启动和销毁，停止，通过addStatementStateListener注册
7: 得到statemnets的结果有3种方式：
   A:注册并继承UpdateListener,可以同时有多个listener,按照注册的顺序来执行
   B:订阅，订阅的类最多一个，效率最高，默认使用update方法，可以指定方法名来执行
   C:需要statement的结果，实现EPStatement的safeIterator【适于多线程处理】 和 iterator【线程不安全】，有order by语句则按照order by的顺序来，否则按照原窗口来
   3者可以任意组合，如果同时有订阅和注册监听器，那么订阅的类会最先得到结果。
15.10：isolated service
   只有隔离内部的statement可见某些事件,默认情况下isolated不可用,可以用于以下场景
    1：不会丢失statement状态的 暂停statement
    2:对某些statement单独提供时间控制,比如模拟，判断到达顺序，计算到达时间，回归测试
    3：事件重放。捕捉历史事件
   EPServiceProviderIsolated的destroy方法会移除所有的isolate服务，使之使用引擎的时间，移除isolated statement，engin保证原有事件的时间和‘时间日历’保持不变