package cn.qiaoxc.demo.group.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class GroupSpout extends BaseRichSpout {

    // 声明发送器对象，用于发送任务
    private SpoutOutputCollector collector;
    // 声明一个计数器
    private int number;


    private String[] arr = new String[]{"aa","bb","cc","dd"};
    /**
     * 打开数据流
     *
     * @param map
     * @param topologyContext     topo上下文对象
     * @param spoutOutputCollector 发送器，将tuple发送到下一个处理器
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    /**
     *
     * 封装tuple
     */
    @Override
    public void nextTuple() {
        this.collector.emit(new Values(number++,arr[(int) (Math.random()*3)]));
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 封装发出流的格式
     *
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("num","word"));
    }
}
