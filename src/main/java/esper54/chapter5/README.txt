1:insert语句代表将当前流的事件定向到另外的流来做为后续处理
2:update语句更新事件的属性，在事件被select statement或者pattern statement处理之前
3:view用来数据查询或者过滤，內建view包括：win:length/std:unique/ext:sort等
4:name window，多个statement可以插入、查询或者删除window里面的数据。本身具有全局属性，整个引擎的所有statement都可以共享
  用于from、join或者子查询语句
5:table拥有主键列，并且能够保持聚合状态的全局可见的数据集
6:on‐demand query即只查询一次,此时查询引擎会自己创建索引
7:schema用来声明时间类型
8:select查询具体事件的属性或者整个事件，from语句查询具体的流，where过滤要查询的事件或其组合
9:多个时间区间的顺序 1 year 1 month 1 week 1 day 1 hour 1 minute 1 sec 1 milliseconds.
10:语句中使用了保留关键字要加上 ` 符号
11:语句本身含有的 “ 或者 ' 需要使用 \ 来还原其原有的含义
12:支持字面符查询（如 1 true）
13:语句支持方法使用并且支持方法链式调用
14:注解区分大小写 todo check注解可以用来干嘛
15:lambda表达式 =>(->等效)左边代表变量，右边代表表达式，使用的时候通过 表达式名字([参数]) 调用，没有参数括号可以不要，表达式本身可以是epl语句
16:istream默认是这种模式，listener通过newEvents接受，oldEvent永远是null;rstream,listener通过newEvents接受；irstream则是各自接受