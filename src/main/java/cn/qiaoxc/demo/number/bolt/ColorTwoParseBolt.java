package cn.qiaoxc.demo.number.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class ColorTwoParseBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Integer count = input.getIntegerByField("count");
        System.out.println("ColorTwoParseBolt---"+count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
