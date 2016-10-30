package esper54.chapter3;

import com.espertech.esper.client.*;

import java.util.concurrent.TimeUnit;

/**
 * Created by liwangchun on 16/10/29.
 */
public class ProcessModel {
    public static void main(String[] args) {
        EPServiceProvider provider = EPServiceProviderManager.getDefaultProvider();
        String eventName = Withdrawal.class.getName();
//        segment32(provider.getEPAdministrator(), provider.getEPRuntime(),eventName);
//        segment33(provider.getEPAdministrator(), provider.getEPRuntime(),eventName);
//        segment34(provider.getEPAdministrator(), provider.getEPRuntime(),eventName);
//        segment352(provider.getEPAdministrator(), provider.getEPRuntime(),eventName);
//        segment36(provider.getEPAdministrator(), provider.getEPRuntime(), eventName);
//        segment3722(provider.getEPAdministrator(), provider.getEPRuntime(), eventName);
//        segment3723(provider.getEPAdministrator(), provider.getEPRuntime(), eventName);
//        segment3724(provider.getEPAdministrator(), provider.getEPRuntime(), eventName);
        segment3725(provider.getEPAdministrator(), provider.getEPRuntime(), WithdrawalOtherValue.class.getName());
    }

    /**
     * 形如 ￼select account, accountName, sum(amount) from Withdrawal.win:time_batch(1 sec) group by account 存在非聚合属性</p>
     * 1：每次sendEvent都会有newEvent产生,只有时间到了，并且上一个窗口有值才会触发oldEvent</p>
     * 2:newEvent 同account来计算smount的和；oldEvent取不到聚合值</p>
     * @param admin
     * @param runtime
     * @param eventName
     */
    public static void segment3725(EPAdministrator admin,EPRuntime runtime,String eventName){
        StringBuilder wBuilder = new StringBuilder("select irstream account,accountName,sum(amount) from ");
        wBuilder.append(eventName);
        wBuilder.append(".win:time_batch(4 sec)");
        wBuilder.append(" group by account");
        String withdrawal = wBuilder.toString();
        EPStatement statement = admin.createEPL(withdrawal);
        statement.addListener(new WithdrawalOtherValueListener());
        WithdrawalOtherValue event = new WithdrawalOtherValue();
        event.setAccount("act1");
        event.setAmount(1000);
        event.setAccountName("maokitty1");
        runtime.sendEvent(event);
        WithdrawalOtherValue event1 = new WithdrawalOtherValue();
        event1.setAccount("act2");
        event1.setAmount(2000);
        event1.setAccountName("maokitty2");
        runtime.sendEvent(event1);
        sleep5Sec();
        WithdrawalOtherValue event2 = new WithdrawalOtherValue();
        event2.setAccount("act4");
        event2.setAmount(3000);
        event2.setAccountName("maokitty3");
        runtime.sendEvent(event2);
        WithdrawalOtherValue event3 = new WithdrawalOtherValue();
        event3.setAccount("act4");
        event3.setAmount(4000);
        event3.setAccountName("maokitty4");
        runtime.sendEvent(event3);
        sleep5Sec();
        WithdrawalOtherValue event4 = new WithdrawalOtherValue();
        event4.setAccount("act5");
        event4.setAmount(5000);
        event4.setAccountName("maokitty5");
        runtime.sendEvent(event4);
        WithdrawalOtherValue event5 = new WithdrawalOtherValue();
        event5.setAccount("act6");
        event5.setAmount(3000);
        event5.setAccountName("maokitty6");
        runtime.sendEvent(event5);
        sleep5Sec();
        sleep5Sec();
    }

    /**
     * 形如 ￼select account, sum(amount) from Withdrawal.win:time_batch(1 sec) group by account 只有普通属性，聚合函数和group by</p>
     * 1:每个event group by属性 同值都会有一行结果给listener</p>
     * 2:普通属性 newEvents和oldEvents都会有值，聚合sum函数是根据account做分组然后做计算，oldEvent拿的是当前窗口中普通属性+上一个窗口聚合值
     *   ；newEvent拿当前窗口中的普通属性+当前窗口聚合值</p>
     * 3:无论是否有新事件send,old event和new Event都会被触发，只是聚合值是否为null</p>
     * @param admin
     * @param runtime
     * @param eventName
     */
    public static void segment3724(EPAdministrator admin,EPRuntime runtime,String eventName){
        StringBuilder wBuilder = new StringBuilder("select irstream account,sum(amount) from ");
        wBuilder.append(eventName);
        wBuilder.append(".win:time_batch(4 sec)");
        wBuilder.append(" group by account");
        String withdrawal = wBuilder.toString();
        EPStatement statement = admin.createEPL(withdrawal);
        statement.addListener(new WithdrawalAccountSumAmoutListener());
        Withdrawal event = new Withdrawal();
        event.setAccount("act1");
        event.setAmount(1000);
        runtime.sendEvent(event);
        Withdrawal event1 = new Withdrawal();
        event1.setAccount("act2");
        event1.setAmount(2000);
        runtime.sendEvent(event1);
        sleep5Sec();
        Withdrawal event2 = new Withdrawal();
        event2.setAccount("act4");
        event2.setAmount(3000);
        runtime.sendEvent(event2);
        Withdrawal event3 = new Withdrawal();
        event3.setAccount("act4");
        event3.setAmount(4000);
        runtime.sendEvent(event3);
        sleep5Sec();
        Withdrawal event4 = new Withdrawal();
        event4.setAccount("act5");
        event4.setAmount(5000);
        runtime.sendEvent(event4);
        Withdrawal event5 = new Withdrawal();
        event5.setAccount("act6");
        event5.setAmount(6000);
        runtime.sendEvent(event5);
        sleep5Sec();
        sleep5Sec();
    }

    /**
     * 形如 ￼select account, sum(amount) from Withdrawal.win:time_batch(1 sec) select语句包含聚合函数和普通属性</p>
     * 1:newEvent获取当前窗口普通属性+当前窗口的聚合值；oldEvent获取上一个窗口的普通属性+当前窗口的聚合值</p>
     * 2：上一个窗口没有值，oldEvent不会被触发，当前窗口没有值不会触发newEvents</p>
     * 3:每次event都会触发一次listener
     * @param admin
     * @param runtime
     * @param eventName
     */
    public static void segment3723(EPAdministrator admin,EPRuntime runtime,String eventName){
        StringBuilder wBuilder = new StringBuilder("select irstream account,sum(amount) from ");
        wBuilder.append(eventName);
        wBuilder.append(".win:time_batch(4 sec)");
        String withdrawal = wBuilder.toString();
        EPStatement statement = admin.createEPL(withdrawal);
        statement.addListener(new WithdrawalAccountSumAmoutListener());
        Withdrawal event = new Withdrawal();
        event.setAccount("act1");
        event.setAmount(1000);
        runtime.sendEvent(event);
        Withdrawal event1 = new Withdrawal();
        event1.setAccount("act2");
        event1.setAmount(2000);
        runtime.sendEvent(event1);
        sleep5Sec();
        Withdrawal event2 = new Withdrawal();
        event2.setAccount("act4");
        event2.setAmount(3000);
        runtime.sendEvent(event2);
        Withdrawal event3 = new Withdrawal();
        event3.setAccount("act4");
        event3.setAmount(4000);
        runtime.sendEvent(event3);
        sleep5Sec();
        Withdrawal event4 = new Withdrawal();
        event4.setAccount("act5");
        event4.setAmount(5000);
        runtime.sendEvent(event4);
        Withdrawal event5 = new Withdrawal();
        event5.setAccount("act6");
        event5.setAmount(6000);
        runtime.sendEvent(event5);
        sleep5Sec();
        sleep5Sec();
    }
    /**
     * 形如 ￼select sum(amount) from Withdrawal.win:time_batch(1 sec) 只有聚合函数 特性</p>
     * 1:使用聚合函数每次触发listener都会有newEvent和oldEvent,new Eevent取当前窗口的值，oldEvent取上一个窗口的值，
     *   会出现oldEvent存在但是里面数据是null,或者newEvent为null</p>
     * 2:select语句只有聚合函数，那么返回的数据是这秒内所有event聚合成一行结果</p>
     * 3:没有取别名，属性的名字就是聚合表达式
     * @param admin
     * @param runtime
     * @param eventName
     */
    public static void segment3722(EPAdministrator admin,EPRuntime runtime,String eventName){
        StringBuilder wBuilder = new StringBuilder("select irstream sum(amount) from ");
        wBuilder.append(eventName);
        wBuilder.append(".win:time_batch(4 sec)");
        String withdrawal = wBuilder.toString();
        EPStatement statement = admin.createEPL(withdrawal);
        statement.addListener(new WithdrawalSumAmoutListener());
        Withdrawal event = new Withdrawal();
        event.setAccount("act1");
        event.setAmount(1000);
        runtime.sendEvent(event);
        Withdrawal event1 = new Withdrawal();
        event1.setAccount("act2");
        event1.setAmount(2000);
        runtime.sendEvent(event1);
        sleep5Sec();
        Withdrawal event2 = new Withdrawal();
        event2.setAccount("act3");
        event2.setAmount(3000);
        runtime.sendEvent(event2);
        Withdrawal event3 = new Withdrawal();
        event3.setAccount("act4");
        event3.setAmount(4000);
        runtime.sendEvent(event3);
        sleep5Sec();
        Withdrawal event4 = new Withdrawal();
        event4.setAccount("act5");
        event4.setAmount(5000);
        runtime.sendEvent(event4);
        Withdrawal event5 = new Withdrawal();
        event5.setAccount("act6");
        event5.setAmount(6000);
        runtime.sendEvent(event5);
        sleep5Sec();
        sleep5Sec();
    }


    /**
     * 形如 ￼select account, amount from Withdrawal.win:time_batch(1 sec) 不包含聚合函数和group 特性</p>
     * 1：listener中收到的类型是Map<String,Object></p>
     * 2：esper会为这秒内的所有的event返回一行结果全部作为newEvent给listener,同时将前一个窗口内的所有数据输出为old event<String,Object></p>
     *
     * @param admin
     * @param runtime
     * @param eventName
     */
    public static void segment36(EPAdministrator admin,EPRuntime runtime,String eventName){
        StringBuilder wBuilder = new StringBuilder("select irstream account,amount from ");
        wBuilder.append(eventName);
        wBuilder.append(".win:time_batch(4 sec)");
        String withdrawal = wBuilder.toString();
        EPStatement statement = admin.createEPL(withdrawal);
        statement.addListener(new WithdrawalBatchpPropertiesListener());
        Withdrawal event = new Withdrawal();
        event.setAccount("act1");
        event.setAmount(1000);
        runtime.sendEvent(event);
        Withdrawal event1 = new Withdrawal();
        event1.setAccount("act2");
        event1.setAmount(2000);
        runtime.sendEvent(event1);
        sleep5Sec();
        Withdrawal event2 = new Withdrawal();
        event2.setAccount("act3");
        event2.setAmount(3000);
        runtime.sendEvent(event2);
        Withdrawal event3 = new Withdrawal();
        event3.setAccount("act4");
        event3.setAmount(4000);
        runtime.sendEvent(event3);
        sleep5Sec();
        Withdrawal event4 = new Withdrawal();
        event4.setAccount("act5");
        event4.setAmount(5000);
        runtime.sendEvent(event4);
        Withdrawal event5 = new Withdrawal();
        event5.setAccount("act6");
        event5.setAmount(6000);
        runtime.sendEvent(event5);
        sleep5Sec();
        sleep5Sec();
    }

    /**
     * 形如 ￼select * from Withdrawal.win:time_batch(4 sec) 特性</p>
     * 1:esper先搜集4s的数据，4s到了之后再批量的发给listener,作为newEvents,如果4s内没有搜集到任何数据，不会触发listener</p>
     * 2:4s时间间隔到了，触发listener批量吐出前一个时间窗口里面的数据，此时时间窗口被清空,如果上一个时间窗口没有数据那么不会触发listener</p>
     * 3:时间窗口是4s A:发送event,调用sleep4 sec后【前后共4s】,再次发送event,可能第一次收不到event，等下一个4slistener收到newEvent[size=4]
     *                或者当前触发listener[size=2]，todo 里面运行代码的时间看具体的esper送数据到listener的机制
     *              B:oldevent同理
     * todo check :rstream只显示出new事件，没有预估的old事件;输出数据受output rate limiting影响;
     *
     * @param admin
     * @param runtime
     * @param eventName
     */
    public static void segment352(EPAdministrator admin,EPRuntime runtime,String eventName){
        StringBuilder wBuilder = new StringBuilder("select irstream * from ");
        wBuilder.append(eventName);
        wBuilder.append(".win:time_batch(5 sec)");
        String withdrawal = wBuilder.toString();
        EPStatement statement = admin.createEPL(withdrawal);
        statement.addListener(new WithdrawalBatchListener());
        Withdrawal event = new Withdrawal();
        event.setAccount("act1");
        event.setAmount(1000);
        runtime.sendEvent(event);
        Withdrawal event1 = new Withdrawal();
        event1.setAccount("act2");
        event1.setAmount(2000);
        runtime.sendEvent(event1);
        sleep5Sec();
        Withdrawal event2 = new Withdrawal();
        event2.setAccount("act3");
        event2.setAmount(3000);
        runtime.sendEvent(event2);
        Withdrawal event3 = new Withdrawal();
        event3.setAccount("act4");
        event3.setAmount(4000);
        runtime.sendEvent(event3);
        sleep5Sec();
        Withdrawal event4 = new Withdrawal();
        event4.setAccount("act5");
        event4.setAmount(5000);
        runtime.sendEvent(event4);
        Withdrawal event5 = new Withdrawal();
        event5.setAccount("act6");
        event5.setAmount(6000);
        runtime.sendEvent(event5);
        sleep5Sec();
        sleep5Sec();
    }

    private static void sleep5Sec(){
        try {
            System.out.println("    sleep 5 sec start");
            TimeUnit.SECONDS.sleep(5);
            System.out.println("    sleep over");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    private static void sleep1Sec(){
        try {
            System.out.println("    sleep 1 sec start");
            TimeUnit.SECONDS.sleep(1);
            System.out.println("    sleep over");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 形如 select * from Withdrawal(amount>=200).win:length(5) 特性</p>
     * amount小于200的不会进入length window</p>
     * 形如 select * from Withdrawal.win:length(5) where amount >= 200</p>
     * amount小于200的会进入window，但不会被updatelistener监听到
     * <p>这里使用的是事件个数窗口，换成事件窗口只需要将后缀改成 .win:time(4 sec)</p>
     * @param admin
     * @param runtime
     * @param eventName
     */
    public static void segment34(EPAdministrator admin,EPRuntime runtime,String eventName){
        StringBuilder wBuilder = new StringBuilder("select * from ");
        wBuilder.append(eventName);
//        wBuilder.append("(amount>=3000).win:length(5)");
        wBuilder.append(".win:length(5) where amount >= 3000");
        String withdrawal = wBuilder.toString();
        EPStatement statement = admin.createEPL(withdrawal);
        statement.addListener(new WithrawalListener());
        Withdrawal event = new Withdrawal();
        event.setAccount("act1");
        event.setAmount(1000);
        runtime.sendEvent(event);
        Withdrawal event1 = new Withdrawal();
        event1.setAccount("act2");
        event1.setAmount(2000);
        runtime.sendEvent(event1);
        Withdrawal event2 = new Withdrawal();
        event2.setAccount("act3");
        event2.setAmount(3000);
        runtime.sendEvent(event2);
        Withdrawal event3 = new Withdrawal();
        event3.setAccount("act4");
        event3.setAmount(4000);
        runtime.sendEvent(event3);
        Withdrawal event4 = new Withdrawal();
        event4.setAccount("act5");
        event4.setAmount(5000);
        runtime.sendEvent(event4);
        Withdrawal event5 = new Withdrawal();
        event5.setAccount("act6");
        event5.setAmount(6000);
        runtime.sendEvent(event5);
    }

    /**
     * 形如 select * from esper54.chapter3.Withdrawal.win:length(5) 特性</p>
     * 1:窗口中值计算5个Withdrawal事件</p>
     * 2:只影响new event,要检测old event需要将语句改成 select irstream * from ...</p>
     * 3:添加irstream后，old监测的时间是最先进来的事件，比如下面old event对应的是 event{act1}
     * @param admin
     * @param runtime
     * @param eventName
     */
    public static void segment33(EPAdministrator admin,EPRuntime runtime,String eventName){
        StringBuilder wBuilder = new StringBuilder("select * from ");
        wBuilder.append(eventName);
        wBuilder.append(".win:length(5)");
        String withdrawal = wBuilder.toString();
        EPStatement statement = admin.createEPL(withdrawal);
        statement.addListener(new WithrawalListener());
        Withdrawal event = new Withdrawal();
        event.setAccount("act1");
        event.setAmount(1000);
        runtime.sendEvent(event);
        Withdrawal event1 = new Withdrawal();
        event1.setAccount("act2");
        event1.setAmount(2000);
        runtime.sendEvent(event1);
        Withdrawal event2 = new Withdrawal();
        event2.setAccount("act3");
        event2.setAmount(3000);
        runtime.sendEvent(event2);
        Withdrawal event3 = new Withdrawal();
        event3.setAccount("act4");
        event3.setAmount(4000);
        runtime.sendEvent(event3);
        Withdrawal event4 = new Withdrawal();
        event4.setAccount("act5");
        event4.setAmount(5000);
        runtime.sendEvent(event4);
        Withdrawal event5 = new Withdrawal();
        event5.setAccount("act6");
        event5.setAmount(6000);
        runtime.sendEvent(event5);
    }


    /**
     * 形如 select * from esper54.chapter3.Withdrawal 特性：</p>
     * 1:只有进入的数据影响updatelistener,不会触发出事件</p>
     * 2:使用通配符，listenner中获取的实例是java bean,直接使用属性的名字(取决于bean的get方法)可获取值</p>
     * todo 事件过多会把内存填满？
     * @param admin
     * @param runtime
     */
    public static void segment32(EPAdministrator admin,EPRuntime runtime,String eventName){
        StringBuilder withdrawalBuilder = new StringBuilder("select * from ");
        withdrawalBuilder.append(eventName);
        String withdrawal = withdrawalBuilder.toString();
        EPStatement statement = admin.createEPL(withdrawal);
        statement.addListener(new WithrawalListener());
        Withdrawal event = new Withdrawal();
        event.setAccount("act1");
        event.setAmount(1000);
        runtime.sendEvent(event);
        Withdrawal event1 = new Withdrawal();
        event1.setAccount("act2");
        event1.setAmount(2000);
        runtime.sendEvent(event1);
    }
}
