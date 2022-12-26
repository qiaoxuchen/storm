package cn.qiaoxc.demo.group;

import cn.qiaoxc.demo.group.bolt.GroupParseBolt;
import cn.qiaoxc.demo.group.spout.GroupSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * 并行度 && 分区策略
 */
public class TopologyGroupApplication {
    public static void main(String[] args) {


        // 创建拓扑图
        TopologyBuilder builder = new TopologyBuilder();

        // 设置拓扑关系 spout
        builder.setSpout("groupSpout", new GroupSpout());

        // 创建2个executor，消费随机分配到groupSpout的数据
//        builder.setBolt("group1Bolt", new GroupOneParseBolt(),2).shuffleGrouping("groupSpout");
        // 创建2个task，一个线程里边运行两个task实例
//        builder.setBolt("group1Bolt", new GroupOneParseBolt()).setNumTasks(2).shuffleGrouping("groupSpout");
//        builder.setBolt("group1Bolt", new GroupOneParseBolt(), 2).setNumTasks(3).shuffleGrouping("groupSpout");

        // 分区策略
//        builder.setBolt("group1Bolt", new GroupParseBolt(), 2).shuffleGrouping("groupSpout");
        builder.setBolt("group1Bolt", new GroupParseBolt(), 6).fieldsGrouping("groupSpout", new Fields("word"));

        Config conf = new Config();
        // 设置worker数
//        conf.setNumWorkers(1);
//        conf.setNumAckers(0);

        if (args != null && args.length > 0) {
            //提交到集群运行
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            //本地模式运行
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("GroupTopology", conf, builder.createTopology());
            Utils.sleep(Long.MAX_VALUE);
            cluster.killTopology("GroupTopology");
            cluster.shutdown();
        }
    }
}
