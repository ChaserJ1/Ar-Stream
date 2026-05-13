package edu.cugb.faft.topology;

import edu.cugb.faft.manager.ApproxBackupManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 解析算子
 * 从原始数据行中提取 taxiId，单发下游
 */
public class SplitBolt extends BaseRichBolt {
    private OutputCollector collector;
    private ApproxBackupManager backupManager;

    public static final int TYPE_REAL = 0;
    public static final int TYPE_APPROX = 1;

    // 判断是否为锚点键 (1% 的数据)
    public static boolean isAnchorKey(String word) {
        return word != null && Math.abs(word.hashCode() % 100) == 0;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            this.backupManager = ApproxBackupManager.getInstance();
        } catch (Exception e) {
            this.backupManager = null;
        }
    }

    @Override
    public void execute(Tuple input) {
        long t0 = System.nanoTime();
        try {
            // 1. 获取输入 (Spout 只发了 sentence)
            String line = input.getStringByField("sentence");

            // 2. 解析 TaxiID，取中间值  数据格式：timestamp, taxiId, ...
            int firstComma = line.indexOf(',');
            int secondComma = line.indexOf(',', firstComma + 1);

            if (firstComma != -1 && secondComma != -1) {
                String taxiId = line.substring(firstComma + 1, secondComma);
                if (!taxiId.isEmpty()) {
                    if (isAnchorKey(taxiId)) {
                        collector.emit(input, new Values(taxiId, TYPE_REAL));
                    }
                    collector.emit(input, new Values(taxiId, TYPE_APPROX));
                }
            }
            collector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            collector.ack(input); // 出错也 Ack，防止卡死
        } finally {
            if (backupManager != null) {
                backupManager.reportExecuteNanos("split-bolt", System.nanoTime() - t0);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "type"));
    }
}
