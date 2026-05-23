package edu.cugb.faft.topology;

import edu.cugb.faft.manager.ApproxBackupManager;
import edu.cugb.faft.monitor.FaftLatencyMonitor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WindowAggregationBolt extends BaseRichBolt {
    private OutputCollector collector;
    private ApproxBackupManager backupManager;
    private String componentId;
    private int taskId;

    private Map<String, Integer> realCounts;
    private Map<String, Integer> approxCounts;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.componentId = context.getThisComponentId();
        this.taskId = context.getThisTaskId();
        this.realCounts = new HashMap<>();
        this.approxCounts = new HashMap<>();

        try {
            this.backupManager = ApproxBackupManager.getInstance();
        } catch (Exception e) {
            this.backupManager = ApproxBackupManager.init(0.5, 0.1, 1.0, 0.05);
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
            System.err.println("[WindowAgg] 配置读取失败: " + e.getMessage());
        }
    }

    @Override
    public void execute(Tuple input) {
        long t0 = System.nanoTime();
        try {
            String word = input.getStringByField("word");
            int type = input.getIntegerByField("type");

            if ("FAFT_CRASH_SIGNAL".equals(word)) {
                if (type == SplitBolt.TYPE_APPROX) {
                    long start = System.currentTimeMillis();
                    this.approxCounts.clear();

                    if (backupManager != null) {
                        Map<String, Integer> backup = backupManager.getBackup(this.componentId, this.taskId);
                        if (backup != null && !backup.isEmpty()) {
                            this.approxCounts.putAll(backup);
                        }
                    }

                    long duration = System.currentTimeMillis() - start;
                    System.out.println("[EXP-METRIC] Type=RTO Time=" + duration + "ms Task=" + taskId + " TS=" + System.currentTimeMillis());

                    FaftLatencyMonitor.checkAndRecordRecovery();
                }
                collector.ack(input);
                return;
            }

            int count;
            if (type == SplitBolt.TYPE_REAL) {
                count = realCounts.getOrDefault(word, 0) + 1;
                realCounts.put(word, count);
            } else {
                count = approxCounts.getOrDefault(word, 0) + 1;
                approxCounts.put(word, count);

                if (backupManager != null) {
                    backupManager.tryBackup(this.componentId, this.taskId, word, count);
                }
            }

            collector.emit(input, new Values(word, count, type));
            collector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        } finally {
            if (backupManager != null) {
                backupManager.reportExecuteNanos(this.componentId, System.nanoTime() - t0);
                backupManager.reportStateSize(this.componentId, realCounts.size() + approxCounts.size());
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count", "type"));
    }
}
