package cn.qiaoxc.demo.reliability.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class AckerLastParseBolt extends BaseRichBolt {

    // 声明发送器对象，用于发送任务
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        System.out.println("-------------------------------2、ackerLastBolt处理过程-------------------------------"+tuple.getMessageId()+"---tuple--:"+tuple);

        // 随机失败
        if (new Random().nextInt(10) == 0) {
            System.out.println("ackerLastBolt处理消息失败：" + tuple);
            this.collector.fail(tuple);
        } else {
            System.out.println("ackerLastBolt处理消息成功：" + tuple);
            this.collector.emit(new Values(tuple.getStringByField("value")));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
