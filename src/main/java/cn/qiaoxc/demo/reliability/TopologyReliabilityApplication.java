package cn.qiaoxc.demo.reliability;

import cn.qiaoxc.demo.reliability.bolt.AckerLastParseBolt;
import cn.qiaoxc.demo.reliability.bolt.AckerParseBolt;
import cn.qiaoxc.demo.reliability.spout.AckerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class TopologyReliabilityApplication {
    public static void main(String[] args) {


        // 创建拓扑图
        TopologyBuilder builder = new TopologyBuilder();

        // 设置拓扑关系 spout
        builder.setSpout("ackerSpout", new AckerSpout());

        // 设置拓扑关系 bolt
        builder.setBolt("ackerBolt", new AckerParseBolt()).shuffleGrouping("ackerSpout");
        builder.setBolt("ackerLastBolt", new AckerLastParseBolt()).shuffleGrouping("ackerBolt");

        Config conf = new Config();
        // 设置worker数
        conf.setNumWorkers(1);
        conf.setNumAckers(1);

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
            cluster.submitTopology("TopoTestStorm", conf, builder.createTopology());
            Utils.sleep(Long.MAX_VALUE);
            cluster.killTopology("TopoTestStorm");
            cluster.shutdown();
        }
    }
}
