package edu.cugb.faft.topology;

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

    // 判断是否为锚点键 (1% 的数据)
    public static boolean isAnchorKey(String word) {
        return word != null && Math.abs(word.hashCode() % 100) == 0;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getStringByField("word");

        // 简单过滤 (例如过滤掉长度小于2的脏数据)
        if (word != null && word.length() > 2) {
            // 1. 正常业务发射 (主流程)
            collector.emit(new Values(word));
            
            // 2. 旁路真值发射 (锚点键)
            if (isAnchorKey(word)) {
                collector.emit("truth-stream", new Values(word));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
        declarer.declareStream("truth-stream", new Fields("word"));
    }
}
