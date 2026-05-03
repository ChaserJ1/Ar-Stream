package edu.cugb.faft.topology;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * 简单自发数据源：每隔 200ms 发射一条句子，字段名为 "sentence"
 * 真实环境替换为外部数据接入。
 */
public class SourceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private final String[] sentences = {
            "the quick brown fox jumps over the lazy dog",
            "storm makes stream processing easy",
            "approximate fault tolerance reduces overhead",
            "distributed systems need reliability",
            "adaptive backup improves efficiency"
    };
    private final Random rand = new Random();

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        String sentence = sentences[rand.nextInt(sentences.length)];
        collector.emit(new Values(sentence), sentence); // 使用 sentence 作为 messageId
        try {
            Thread.sleep(200);
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void ack(Object msgId) {
        /* 可选日志 */ }

    @Override
    public void fail(Object msgId) {
        /* 可选重发 */
        collector.emit(new Values(String.valueOf(msgId)), msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
