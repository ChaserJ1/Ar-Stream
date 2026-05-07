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

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getStringByField("word");
        int type = input.getIntegerByField("type");

        // 简单过滤 (例如过滤掉长度小于2的脏数据)
        if (word != null && word.length() > 2) {
            collector.emit(new Values(word, type));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "type"));
    }
}
