package edu.cugb.faft.topology;

import edu.cugb.faft.manager.ApproxBackupManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class JoinBolt extends BaseRichBolt {
    private OutputCollector collector;
    private ApproxBackupManager backupManager;
    private String componentId;

    private Map<String, Long> joinState;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.componentId = context.getThisComponentId();
        this.joinState = new HashMap<>();

        try {
            this.backupManager = ApproxBackupManager.getInstance();
        } catch (Exception e) {
            this.backupManager = ApproxBackupManager.init(1.0, 0.1, 1.0, 0.05);
        }

        try {
            if (topoConf.containsKey("faft.ratios")) {
                Object ratiosObj = topoConf.get("faft.ratios");
                if (ratiosObj instanceof Map) {
                    this.backupManager.updateSamplingRatios((Map<String, Double>) ratiosObj);
                }
            }
            if (topoConf.containsKey("faft.importance")) {
                Object impObj = topoConf.get("faft.importance");
                if (impObj instanceof Map) {
                    Map<String, Object> rawMap = (Map<String, Object>) impObj;
                    Map<String, Double> impMap = new HashMap<>();
                    for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
                        try {
                            impMap.put(entry.getKey(), Double.parseDouble(String.valueOf(entry.getValue())));
                        } catch (NumberFormatException ignored) {}
                    }
                    this.backupManager.updateImportance(impMap);
                }
            }
        } catch (Exception e) {
            System.err.println("[JoinBolt] 配置读取失败: " + e.getMessage());
        }
    }

    @Override
    public void execute(Tuple input) {
        long t0 = System.nanoTime();
        try {
            String word = input.getStringByField("word");
            int type = input.getIntegerByField("type");

            joinState.put(word, System.currentTimeMillis());

            collector.emit(input, new Values(word, type));
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
        } finally {
            if (backupManager != null) {
                backupManager.reportExecuteNanos(this.componentId, System.nanoTime() - t0);
                backupManager.reportStateSize(this.componentId, joinState.size());
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "type"));
    }
}
