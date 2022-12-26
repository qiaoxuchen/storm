package cn.qiaoxc.demo.group.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class GroupOneParseBolt extends BaseBasicBolt {
    /**
     * 处理数据
     *
     * @param tuple
     * @param basicOutputCollector
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        System.out.println("GroupOneParseBolt接收上游的值：【" + this + "】【" + tuple + "】");
    }

    /**
     * 处理向下游传递的数据
     *
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
