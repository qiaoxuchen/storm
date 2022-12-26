package cn.qiaoxc.demo.group.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class GroupThreeParseBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        System.out.println("ColorThreeParseBolt接收上游的值：" + tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
