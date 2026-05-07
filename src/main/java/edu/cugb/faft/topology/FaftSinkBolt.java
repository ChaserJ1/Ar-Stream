package edu.cugb.faft.topology;

import edu.cugb.faft.importance.NodeImportanceEvaluator;
import edu.cugb.faft.manager.ApproxBackupManager;
import edu.cugb.faft.monitor.FaftLatencyMonitor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

import static org.apache.commons.lang3.math.NumberUtils.toDouble;

@SuppressWarnings("unchecked")
public class FaftSinkBolt extends BaseRichBolt {
    private OutputCollector collector; // 用于 ack/fail
    private ApproxBackupManager backupManager;
    private String operatorId; // 记录该算子的 componentId，供按算子采样

    // 日志窗口统计
    private int counter = 0;
    private static final int LOG_WINDOW = 5000; // 日志打印间隔

    // 全局误差追踪 (锚点键)
    private Map<String, Integer> globalRealView;
    private Map<String, Integer> globalApproxView;
    private double errorThreshold = 0.05;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        this.operatorId = context.getThisComponentId();
        System.out.println("[FAFT BoltInit] sink componentId=" + this.operatorId);

        this.globalRealView = new HashMap<>();
        this.globalApproxView = new HashMap<>();
        this.errorThreshold = getDouble(topoConf, "faft.error.threshold", 0.05);

        // 1. Zookeeper 监控初始化 (修复配置失效问题)
        String zkConnect = (String) topoConf.get("faft.zk.connect");
        if (zkConnect == null)
            zkConnect = "192.168.213.130:2181"; // 默认兜底
        FaftLatencyMonitor.setZkConnect(zkConnect);

        try {
            this.backupManager = ApproxBackupManager.getInstance(); // 单例共享
        } catch (IllegalStateException e) {
            // 如果还没初始化，则用默认参数兜底初始化一次
            this.backupManager = ApproxBackupManager.init(
                    0.5, // 初始采样率
                    0.1, // 最小采样率
                    1.0, // 最大采样率
                    0.05 // 步长
            );
        }

        // 2. 接收初始采样率与重要性 (来自 Launcher 的计算结果)
        try {
            Object ratios = topoConf.get("faft.ratios");
            if (ratios instanceof Map) {
                this.backupManager.updateSamplingRatios((Map<String, Double>) ratios);
                System.out.println("[FAFT RatioUpdate] sink received ratios=" + ratios);
            }

            Object impObj = topoConf.get("faft.importance");
            if (impObj instanceof Map) {
                // 增加类型转换容错
                Map<String, String> impStrMap = (Map<String, String>) impObj;
                Map<String, Double> impDoubleMap = new HashMap<>();
                for (Map.Entry<String, String> e : impStrMap.entrySet()) {
                    try {
                        impDoubleMap.put(e.getKey(), Double.parseDouble(e.getValue().toString()));
                    } catch (Exception ignore) {
                    }
                }
                this.backupManager.updateImportance(impDoubleMap);
                System.out.println("[FAFT ImportanceUpdate][sink] " + impDoubleMap);
            }
        } catch (Exception e) {
            System.err.println("⚠️ [Sink] 配置读取部分失败: " + e.getMessage());
        }

        // 3. 解析算法参数 (差异化权重 + 超参)
        // 3.1 全局默认权重
        double defAlpha = getDouble(topoConf, "faft.alpha", 0.34);
        double defBeta = getDouble(topoConf, "faft.beta", 0.33);
        double defGamma = getDouble(topoConf, "faft.gamma", 0.33);
        NodeImportanceEvaluator.Weights defaultWeights = new NodeImportanceEvaluator.Weights(defAlpha, defBeta,
                defGamma);

        // 3.2 差异化权重 Map (解析 List<Double> -> Weights)
        Map<String, NodeImportanceEvaluator.Weights> weightsObjMap = new HashMap<>();
        Object wObj = topoConf.get("faft.weights");
        if (wObj instanceof Map) {
            try {
                Map<String, List<Double>> rawWeights = (Map<String, List<Double>>) wObj;
                for (Map.Entry<String, List<Double>> e : rawWeights.entrySet()) {
                    List<Double> val = e.getValue();
                    if (val != null && val.size() >= 3) {
                        weightsObjMap.put(e.getKey(),
                                new NodeImportanceEvaluator.Weights(val.get(0), val.get(1), val.get(2)));
                    }
                }
            } catch (Exception e) {
                System.err.println("⚠️ 权重解析警告: " + e.getMessage());
            }
        }

        // 3.3 其他算法超参
        double impactDelta = getDouble(topoConf, "faft.impact.delta", 0.9);
        double decayAlpha = getDouble(topoConf, "faft.decay.alpha", 0.9);
        double rmin = getDouble(topoConf, "faft.rmin", 0.1);
        double rmax = getDouble(topoConf, "faft.rmax", 1.0);

        // 4. 准备 DAG & 启动动态重算
        Map<String, List<String>> dag = null;
        List<String> sinkList = null;
        try {
            dag = (Map<String, List<String>>) topoConf.get("faft.dag");
            sinkList = (List<String>) topoConf.get("faft.sinks");
        } catch (Exception e) {
        }

        // 简单的本地兜底，防止 Config 读取失败导致空指针
        if (dag == null || dag.isEmpty()) {
            System.err.println("[FAFT-ERROR] DAG not found in config! Using fallback.");

            dag = new HashMap<>();
            dag.put("source-spout", List.of("split-bolt"));
            dag.put("split-bolt", List.of("filter-bolt"));
            dag.put("filter-bolt", List.of("chaos-bolt"));
            dag.put("chaos-bolt", List.of("faft-count-bolt"));
            dag.put("faft-count-bolt", List.of("faft-sink-bolt"));
            dag.put("faft-sink-bolt", List.of());
            sinkList = List.of("faft-sink-bolt");
        }
        // 二次检查
        if (sinkList == null)
            sinkList = List.of("faft-sink-bolt");

        Set<String> sinks = new HashSet<>(sinkList);

        // 启动动态重算 (传入解析好的 Weights Map)
        this.backupManager.startDynamicRebalance(
                dag, sinks,
                weightsObjMap, // 差异化权重
                defaultWeights, // 默认权重
                impactDelta, decayAlpha,
                rmin, rmax,
                10_000L // 重算周期 10s
        );
    }

    @Override
    public void execute(Tuple input) {
        try {
            String word = input.getStringByField("word");
            int count = input.getIntegerByField("count");
            int type = input.getIntegerByField("type");

            if (type == SplitBolt.TYPE_REAL) {
                globalRealView.put(word, count);
            } else {
                // 对于 APPROX 数据，我们只关心锚点键的近似值用于 MRE 计算
                if (SplitBolt.isAnchorKey(word)) {
                    globalApproxView.put(word, count);
                }
            }

            counter++;
            if (counter >= LOG_WINDOW) {
                calculateAndAdjustGlobalMRE();
                counter = 0;
            }

            collector.ack(input);
        } catch (Exception e) {
            collector.ack(input);
        }
    }

    private void calculateAndAdjustGlobalMRE() {
        if (globalRealView.isEmpty()) return;
        
        double totalError = 0.0;
        int items = 0;
        
        for (Map.Entry<String, Integer> entry : globalRealView.entrySet()) {
            String key = entry.getKey();
            double truthVal = entry.getValue();
            double approxVal = globalApproxView.getOrDefault(key, 0);
            
            if (truthVal > 0) {
                totalError += Math.abs(truthVal - approxVal) / truthVal;
                items++;
            }
        }
        
        double globalMRE = (items == 0) ? 0.0 : totalError / items;
        
        System.out.printf("[Sink] 已处理 %d 条结果 | 当前全局 MRE=%.4f%% (阈值=%.2f%%, 锚点数=%d)%n", 
                LOG_WINDOW, globalMRE * 100, errorThreshold * 100, items);
                
        if (backupManager != null) {
            backupManager.adjustByError(globalMRE, errorThreshold);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    /**
     * 辅助方法：安全获取 Config 中的 double
     */
    private double getDouble(Map<String, Object> map, String key, double defaultValue) {
        if (map == null)
            return defaultValue;
        Object val = map.get(key);
        return toDouble(String.valueOf(val), defaultValue);
    }
}
