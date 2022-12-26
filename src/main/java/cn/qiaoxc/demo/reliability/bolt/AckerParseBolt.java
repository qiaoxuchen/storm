package cn.qiaoxc.demo.reliability.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class AckerParseBolt extends BaseRichBolt {

    // 声明发送器对象，用于发送任务
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        System.out.println("-------------------------------1、AckerBolt处理过程-------------------------------"+tuple.getMessageId()+"---tuple--:"+tuple);

        // 随机失败
        if (new Random().nextInt(10) == 0) {
            System.out.println("AckerBolt处理失败：" + tuple);
            this.collector.fail(tuple);
        } else {
            System.out.println("AckerBolt处理成功：" + tuple);
            // 建立tuple tree
            this.collector.emit(tuple ,new Values(tuple.getStringByField("value")));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("value"));

    }

}
