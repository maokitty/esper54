1:Every 操作符
假设事件发生的顺序是 A1 B1 C1 B2 A2 D1 A3 B3 E1 A4 F1 B4
 every (A->B )
    匹配组合 B1到达则{A1, B1} B3到达{A2, B3} B4到达{A4, B4}
 every A->B
    匹配组合 B1到达{A1, B1} B3到达{A2, B3}和{A3, B3} B4到达{A4, B4}
 A-> every B
    匹配组合 B1到达{A1, B1} B2到达{A1, B2} B3到达{A1, B3} B4到达{A1, B4}
 every A -> every B
    匹配组合 B1到达{A1, B1} B2到达{A1, B2} B3到达{A1, B3},{A2, B3},{A3, B3} B4到达{A1, B4},{A2, B4},{A3, B4},{A1, B4}
假设事件发生顺序是 A1 A2 B1
 every a=A->b=B
    匹配 {A1, B1} {A2,B1}
 every a=A -> (b=B and not A)
    匹配 {A2,B1}

pattern中的表达式如果只有every操作符和单个的过滤表达式相当于从from语句中过滤
    select * from StockTickEvent(symbol='GE') 【最好这么使用】相当于 select * from pattern[every StockTickEvent(symbol='GE')]


timer:schedule 与 timer:at
   timer:at与crontab类似；timer:schedule则是指定时间戳