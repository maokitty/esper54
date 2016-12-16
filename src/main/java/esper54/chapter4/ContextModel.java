package esper54.chapter4;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import esper54.Util.PatternCommonListener;
import esper54.Util.TimeUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by liwangchun on 16/12/15.
 */
public class ContextModel {
    public static void main(String[] args) {
        EPServiceProvider provider = EPServiceProviderManager.getDefaultProvider();
        segment422(provider.getEPAdministrator(),provider.getEPRuntime());
    }

    /**
     * custId不同不会触发
     * @param admin
     * @param runtime
     */
    public static void segment422(EPAdministrator admin,EPRuntime runtime){
        admin.createEPL("create schema BankTxn (custId long,account string,amount double)");
        admin.createEPL("create context SegmentedByCustomer partition by custId from BankTxn");
        //where timer:within(3 seconds) 只匹配接下来的3秒，之后的不再输出
        admin.createEPL("context SegmentedByCustomer select a,b from pattern [every a=BankTxn(amount>400)->b=BankTxn(amount>400) where timer:within(3 seconds)]").addListener(new PatternCommonListener());
        Map<String,Object> o1=getBankTxn(1,"maokitty1",500);
        Map<String,Object> o2=getBankTxn(1,"maokitty2",600);
        Map<String,Object> o3=getBankTxn(1,"maokitty3",700);
        Map<String,Object> o4=getBankTxn(1,"maokitty3",800);
        runtime.sendEvent(o1,"BankTxn");
        runtime.sendEvent(o2,"BankTxn");
        runtime.sendEvent(o3,"BankTxn");
        TimeUtil.sleepSec(4);
        runtime.sendEvent(o4,"BankTxn");
    }
    private static Map<String,Object> getBankTxn(long custId,String account,double amount){
        Map<String,Object> obj=new HashMap<String, Object>();
        obj.put("custId",custId);
        obj.put("account",account);
        obj.put("amount",amount);
        return obj;
    }
}
