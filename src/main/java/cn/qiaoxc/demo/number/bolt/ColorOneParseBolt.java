package cn.qiaoxc.demo.number.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ColorOneParseBolt extends BaseBasicBolt {

    int count = 0;
    /**
     * 处理数据
     *
     * @param tuple
     * @param basicOutputCollector
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        System.out.println("ColorOneParseBolt接收上游的值：" + tuple);
        count += tuple.getInteger(0);
        basicOutputCollector.emit(new Values(count));
    }

    /**
     * 处理向下游传递的数据
     *
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("count"));
    }
}
