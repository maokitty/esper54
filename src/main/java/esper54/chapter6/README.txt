1:window和table的区别
  window更适合于多个statements之间共享数据  用来存储事件                iterator返回的顺序和event进入顺序有关  能返回insert remove流         每个update event都会有逻辑上的copy操作，只会修改最新的事件，保留原始不变的额事件  window本身能够直接删除事件，同时 on-mergeon-delete fire-and-forge delete也行；
  table适合存储有主键或者存储集合状态的数据   可添加其它的状态（聚合状态）  table返回的顺序则不可预测              table不能返回insert remove流  直接修改原有的事件                                                         只能on-mergeon-delete fire-and-forge delete
2:window
  创建的时候window里面没有数据，数据必须通过statement执行insert才能流入。如果创建window的statement被停止或者销毁
3:table
  table不会产生insert和remove流，如果from语句中只有table，statement输出只有两种方式：1，通过一次性查询；2：通过pull API
  table不能用在 先匹配查找然后识别输出的statement，context声明，pattern原子过滤和update istream
  table可用在子查询和joins中
4: on系列
  on-merge 存在就更新，不存在就插入
  on-select 一次性非连续性的查询，当触发事件或者触发pattern满足时发生
  on-select-delete 将选中的行删除
  on-update 事件触发的时候更新table或者window中的匹配的所有行
  on-delete 事件到来直接删除