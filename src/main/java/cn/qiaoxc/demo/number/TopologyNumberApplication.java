package cn.qiaoxc.demo.number;

import cn.qiaoxc.demo.number.bolt.ColorOneParseBolt;
import cn.qiaoxc.demo.number.bolt.ColorThreeParseBolt;
import cn.qiaoxc.demo.number.bolt.ColorTwoParseBolt;
import cn.qiaoxc.demo.number.spout.ColorSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class TopologyNumberApplication {
    public static void main(String[] args) {


        // 创建拓扑图
        TopologyBuilder builder = new TopologyBuilder();

        // 设置拓扑关系 spout
        builder.setSpout("colorSpout", new ColorSpout());

        // 设置拓扑关系 bolt
        builder.setBolt("color1Bolt", new ColorOneParseBolt()).shuffleGrouping("colorSpout");
        builder.setBolt("color2Bolt", new ColorTwoParseBolt()).shuffleGrouping("color1Bolt");
        builder.setBolt("color3Bolt", new ColorThreeParseBolt()).shuffleGrouping("colorSpout");

        Config conf = new Config();
        // 设置worker数
        conf.setNumWorkers(2);
        conf.setNumAckers(0);

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
