package edu.cugb.faft.topology;

import edu.cugb.faft.monitor.DefaultLatencyMonitor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * 默认下沉算子（storm默认容错策略使用）
 * 输入字段：word, count
 */
public class SinkBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        DefaultLatencyMonitor.init();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        int count = tuple.getIntegerByField("count");
        System.out.printf("[Default Sink] Word=%s | Count=%d%n", word, count);

        DefaultLatencyMonitor.checkAndRecordRecovery();

        collector.ack(tuple);
    }

    @Override

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
