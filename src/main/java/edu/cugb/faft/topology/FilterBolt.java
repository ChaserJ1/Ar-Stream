package edu.cugb.faft.topology;

import edu.cugb.faft.manager.ApproxBackupManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * 过滤算子
 * 输入字段：word
 * 输出字段：word
 * 作用：过滤掉长度小于2的脏数据
 */

public class FilterBolt extends BaseBasicBolt {

    private transient ApproxBackupManager backupManager;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        try {
            this.backupManager = ApproxBackupManager.getInstance();
        } catch (Exception e) {
            this.backupManager = null;
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        long t0 = System.nanoTime();
        try {
            String word = input.getStringByField("word");
            int type = input.getIntegerByField("type");

            // 简单过滤 (例如过滤掉长度小于2的脏数据)
            if (word != null && word.length() > 2) {
                collector.emit(new Values(word, type));
            }
        } finally {
            if (backupManager != null) {
                backupManager.reportExecuteNanos("filter-bolt", System.nanoTime() - t0);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "type"));
    }
}
