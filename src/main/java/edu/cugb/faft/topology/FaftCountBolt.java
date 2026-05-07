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

public class FaftCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private ApproxBackupManager backupManager;
    private String componentId;

    private int taskId; // 集群中区分不同实例

    // 微型双轨状态
    private Map<String, Integer> realCounts;
    private Map<String, Integer> approxCounts;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.componentId = context.getThisComponentId();
        this.taskId = context.getThisTaskId(); // 获取当前 Task ID
        this.realCounts = new HashMap<>();
        this.approxCounts = new HashMap<>();

        // 1. 先使用默认参数初始化 BackupManager，防止配置读取失败导致空指针
        try {
            // 尝试获取单例
            this.backupManager = ApproxBackupManager.getInstance();
        } catch (Exception e) {
            // 如果还没初始化，用默认参数兜底 (0.5采样率)
            this.backupManager = ApproxBackupManager.init(0.5, 0.1, 1.0, 0.05);
        }

        // 2. 尝试从 Storm Config 读取 Launcher 下发的配置
        try {
            // 读取采样率表 (Ratios)
            if (topoConf.containsKey("faft.ratios")) {
                Object ratiosObj = topoConf.get("faft.ratios");
                if (ratiosObj instanceof Map) {
                    Map<String, Double> ratios = (Map<String, Double>) ratiosObj;
                    this.backupManager.updateSamplingRatios(ratios);
                    System.out.println("[FaftCount] 成功加载初始采样率: " + ratios);
                }
            }

            // 读取重要性表 (Importance)
            if (topoConf.containsKey("faft.importance")) {
                Object impObj = topoConf.get("faft.importance");
                if (impObj instanceof Map) {
                    Map<String, Object> rawMap = (Map<String, Object>) impObj;
                    Map<String, Double> impMap = new HashMap<>();

                    for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
                        try {
                            // 强转 Double，兼容 String 类型
                            double val = Double.parseDouble(String.valueOf(entry.getValue()));
                            impMap.put(entry.getKey(), val);
                        } catch (NumberFormatException ignored) {}
                    }

                    this.backupManager.updateImportance(impMap);
                    System.out.println("[FaftCount] 成功加载节点重要性: " + impMap);
                }
            }
        } catch (Exception e) {
            System.err.println("[FaftCount] 读取配置失败，将使用默认参数运行。错误: " + e.getMessage());
            // 不抛出异常，保证程序能继续跑
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            String word = input.getStringByField("word");
            int type = input.getIntegerByField("type");

            // ============================================
            // 1. 崩溃信号处理
            // ============================================
            if ("FAFT_CRASH_SIGNAL".equals(word)) {
                // 只有 TYPE_APPROX 受到崩溃影响
                if (type == SplitBolt.TYPE_APPROX) {
                    // [实验埋点] 开始计时
                    long start = System.currentTimeMillis();
                    System.out.printf("[FaftCount] Task-%d 收到崩溃信号！模拟内存丢失...%n", taskId);

                    // 1. 模拟状态丢失（清空近似状态，不影响真值状态）
                    this.approxCounts.clear();

                    // 2. 从Redis近似备份中恢复 (Restore)
                    if (backupManager != null) {
                        Map<String, Integer> backup = backupManager.getBackup(this.componentId, this.taskId);
                        if (backup != null && !backup.isEmpty()) {
                            this.approxCounts.putAll(backup);
                            System.out.printf("[FaftCount] Task-%d 从 Redis 恢复 %d 项%n", taskId, backup.size());
                        }
                    }

                    // [实验埋点] 结束计时，打印 RTO
                    long duration = System.currentTimeMillis() - start;
                    System.out.println("[EXP-METRIC] Type=RTO Time=" + duration + "ms Task=" + taskId + " TS=" + System.currentTimeMillis());

                    // 3. 记录恢复完成时间 (供 Monitor 计算 Latency)
                    FaftLatencyMonitor.checkAndRecordRecovery();
                }

                collector.ack(input);
                return; // 信号本身不计入统计
            }

            // ============================================
            // 2. 正常计数 + 采样备份
            // ============================================
            int count;
            if (type == SplitBolt.TYPE_REAL) {
                count = realCounts.getOrDefault(word, 0) + 1;
                realCounts.put(word, count);
                // 真值流不需要存入 Redis 进行近似容错，因为它在内存中受锚点保护不丢失
            } else {
                count = approxCounts.getOrDefault(word, 0) + 1;
                approxCounts.put(word, count);

                // 尝试备份 (Backup Strategy)
                backupManager.tryBackup(this.componentId, this.taskId, word, count);
            }

            // ============================================
            // 3. 发射结果
            // ============================================
            collector.emit(input, new Values(word, count, type));
            collector.ack(input);

        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 输出格式: 单词, 计数值, 类型
        declarer.declare(new Fields("word", "count", "type"));
    }
}