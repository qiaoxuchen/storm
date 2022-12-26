package cn.qiaoxc.demo.reliability.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class AckerSpout extends BaseRichSpout {

    // 声明发送器对象，用于发送任务
    private SpoutOutputCollector collector;
    // 声明一个发送存储器 <msgId,Values>
    private ConcurrentHashMap<Object, Values> ackMap = new ConcurrentHashMap<>();
    // 声明一个失败存储器  <msgId,失败次数>
    private ConcurrentHashMap<Object, Integer> failMap = new ConcurrentHashMap<>();

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        int messageId = new Random().nextInt(Integer.MAX_VALUE);
        Values values = new Values(uuid);
        // 发送消息
        this.collector.emit(values, messageId);
        // 将发送过的消息，存入map
        ackMap.put(messageId, values);
        System.out.println("AskerSpout开始发出消息,messageID=" + messageId + "--value=" + uuid);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("value"));
    }

    /**
     * bolt处理tuple成功，回调此方法
     *
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        this.ackMap.remove(msgId);
        this.failMap.remove(msgId);
        System.out.println("failMap size 剩余:" + failMap.values());
    }

    /**
     * bolt处理tuple失败，回调此方法，决定重试还是放弃
     *
     * @param msgId 消息ID
     */
    @Override
    public void fail(Object msgId) {
        System.out.println("-------------------------------3、spout fail-------------------------------:" + msgId.toString());
        Integer fail_count = failMap.get(msgId); //获取该Tuple失败的次数
        if (fail_count == null) {
            fail_count = 0;
        }
        fail_count++;
        if (fail_count >= 3) {
            //重试次数已满，不再进行重新emit
            failMap.remove(msgId);
            System.out.println(msgId+"-重试次数已满,移除map");
        } else {
            //记录该tuple失败次数
            failMap.put(msgId, fail_count);
            //重发
            this.collector.emit(this.ackMap.get(msgId), msgId);
            System.out.println(msgId+"-处理失败，重试");
        }
    }


}
