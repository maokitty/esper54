package esper54.chapter6;

import com.espertech.esper.client.*;
import esper54.Util.CommonListener;
import esper54.Util.PrintTableResult;
import esper54.Util.TimeUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by liwangchun on 16/12/6.
 * table 不提供insert和remove流，如果from语句中只有table名，那么要获取结果只能通过迭代(pull API)或者on-demand（见15章）查询
 */
public class TableModel {
    public static void main(String[] args) {
        EPServiceProvider provider = EPServiceProviderManager.getDefaultProvider();
//        segment632(provider.getEPAdministrator(), provider.getEPRuntime());
        segment633(provider.getEPAdministrator(), provider.getEPRuntime());
    }

    /**
     * 使用这种表达式，必须table中必须有primary key，查询的时候，主键没有找到会返回null
     * @param admin
     * @param runtime
     */

    public static void segment633(EPAdministrator admin, EPRuntime runtime) {
        admin.createEPL("create schema FirewallEvent as(`from` string,to string)");
        admin.createEPL("create table InstrusionCountTable (" +
                "fromAddress string primary key," +
                "toAddress string primary key," +
                "countIntrusion10Sec count(*))");
        //容易出现主键冲突，使用on-merge比较好[segment68]
        admin.createEPL("insert into InstrusionCountTable select `from` as fromAddress,to as toAddress from FirewallEvent");
        admin.createEPL("insert into InstrusionCountTable select count(*) as countIntrusion10Sec from FirewallEvent.win:time(60) group by to,`from`");
        admin.createEPL("select InstrusionCountTable[`from`,to].countIntrusion10Sec from FirewallEvent").addListener(new CommonListener());
        Map<String,Object> firewallMap=getFirewallEvent("10.4.245.12", "10.4.245.16");
        runtime.sendEvent(firewallMap,"FirewallEvent");
    }

    /**
     * table
     * table没有primary key，里面要么存null,要么就只有一行数据
     *
     * @param admin
     * @param runtime
     */
    public static void segment632(EPAdministrator admin, EPRuntime runtime) {
        admin.createEPL("create schema InstrusionEvent as(fromAddress string,toAddress string)");
        admin.createEPL("create table InstrusionCountTable (" +
                "fromAddress string primary key," +
                "toAddress string primary key," +
                "countIntrusion10Sec count(*)," +
                "countIntrusion60Sec count(*))");
        admin.createEPL("into table InstrusionCountTable select count(*) as countIntrusion10Sec from InstrusionEvent.win:time(10) group by fromAddress,toAddress");
        admin.createEPL("into table InstrusionCountTable select count(*) as countIntrusion60Sec from InstrusionEvent.win:time(60) group by fromAddress,toAddress");
        Map<String, Object> ins0 = getInstrusionEvent("10.4.233.10", "10.4.233.11");
        Map<String, Object> ins1 = getInstrusionEvent("10.4.233.10", "10.4.233.12");
        Map<String, Object> ins2 = getInstrusionEvent("10.4.233.10", "10.4.233.13");
        String query = "select * from InstrusionCountTable";
        runtime.sendEvent(ins0, "InstrusionEvent");
        runtime.sendEvent(ins1, "InstrusionEvent");
        EPOnDemandQueryResult result = runtime.executeQuery(query);
        PrintTableResult.execute(result);
        TimeUtil.sleepSec(11);
        runtime.sendEvent(ins2, "InstrusionEvent");
        runtime.sendEvent(ins2, "InstrusionEvent");
        result = runtime.executeQuery(query);
        PrintTableResult.execute(result);
    }

    private static Map<String, Object> getInstrusionEvent(String fromAdd, String toAdd) {
        Map<String, Object> instrusion = new HashMap<String, Object>();
        instrusion.put("fromAddress", fromAdd);
        instrusion.put("toAddress", toAdd);
        return instrusion;
    }
    private static Map<String, Object> getFirewallEvent(java.lang.String fromAdd, java.lang.String toAdd) {
    Map<String, Object> firewall = new HashMap<String, Object>();
        firewall.put("from", fromAdd);
        firewall.put("to", toAdd);
        return firewall;
    }
}