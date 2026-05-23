package edu.cugb.faft.topology;

import edu.cugb.faft.importance.NodeImportanceEvaluator;
import edu.cugb.faft.importance.OperatorInfo;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.*;

import static org.apache.commons.lang3.math.NumberUtils.toDouble;

public class YSBTopologyLauncher {

    @SuppressWarnings("unchecked")
    private static Map<String, Object> loadFaftConfig() {
        Yaml yaml = new Yaml();
        try (InputStream in = YSBTopologyLauncher.class.getClassLoader().getResourceAsStream("application.yml")) {
            if (in == null) {
                System.err.println("[YSB-WARN] application.yml 获取失败，将使用默认参数");
                return null;
            }
            Map<String, Object> obj = yaml.load(in);
            return (Map<String, Object>) obj.get("faft");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source-spout", new FileSourceSpout("/opt/data/faft.txt", true), 1);

        builder.setBolt("event-filter-bolt", new EventFilterBolt(), 2)
                .shuffleGrouping("source-spout");

        builder.setBolt("project-bolt", new ProjectBolt(), 2)
                .shuffleGrouping("event-filter-bolt");

        builder.setBolt("split-bolt", new SplitBolt(), 2)
                .shuffleGrouping("project-bolt");

        builder.setBolt("chaos-bolt", new ChaosBolt(0.0001, 0, 50), 2)
                .shuffleGrouping("split-bolt");

        builder.setBolt("join-bolt", new JoinBolt(), 2)
                .fieldsGrouping("chaos-bolt", new Fields("word"));

        builder.setBolt("window-agg-bolt", new WindowAggregationBolt(), 2)
                .fieldsGrouping("join-bolt", new Fields("word"));

        builder.setBolt("faft-sink-bolt", new FaftSinkBolt(), 1)
                .globalGrouping("window-agg-bolt");

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(2);
        conf.setMessageTimeoutSecs(30);
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 1.0);

        double defAlpha = 0.34;
        double defBeta = 0.33;
        double defGamma = 0.33;

        double impactDelta = 0.9;
        double omegaIn = 0.5;
        double omegaOut = 0.5;
        double errorThreshold = 0.10;

        double rmin = 0.1;
        double rmax = 1.0;
        double step = 0.05;

        String zkConnect = "127.0.0.1:2181";

        Map<String, List<Double>> rawWeightsMap = new HashMap<>();

        Map<String, Object> faftConfig = loadFaftConfig();
        if (faftConfig != null) {
            System.out.println("[YSB-INFO] 成功加载 application.yml 配置：" + faftConfig);
            defAlpha = getDouble(faftConfig, "alpha", defAlpha);
            defBeta = getDouble(faftConfig, "beta", defBeta);
            defGamma = getDouble(faftConfig, "gamma", defGamma);

            impactDelta = getDouble(faftConfig, "impact-delta", impactDelta);
            omegaIn = getDouble(faftConfig, "omega-in", omegaIn);
            omegaOut = getDouble(faftConfig, "omega-out", omegaOut);
            errorThreshold = getDouble(faftConfig, "error-threshold", errorThreshold);

            rmin = getDouble(faftConfig, "rmin", rmin);
            rmax = getDouble(faftConfig, "rmax", rmax);
            step = getDouble(faftConfig, "step", step);

            zkConnect = (String) faftConfig.getOrDefault("zk-connect", zkConnect);

            if (faftConfig.containsKey("ysb-weights")) {
                Map<String, List<Double>> wCfg = (Map<String, List<Double>>) faftConfig.get("ysb-weights");
                if (wCfg != null) {
                    rawWeightsMap.putAll(wCfg);
                }
            }
        }

        conf.put("faft.alpha", defAlpha);
        conf.put("faft.beta", defBeta);
        conf.put("faft.gamma", defGamma);

        conf.put("faft.impact.delta", impactDelta);
        conf.put("faft.omega.in", omegaIn);
        conf.put("faft.omega.out", omegaOut);
        conf.put("faft.error.threshold", errorThreshold);

        conf.put("faft.rmin", rmin);
        conf.put("faft.rmax", rmax);
        conf.put("faft.step", step);

        conf.put("faft.zk.connect", zkConnect);
        conf.put("faft.weights", rawWeightsMap);

        int verifyInterval = 10000;
        int verifyDuration = 1000;
        if (faftConfig != null) {
            verifyInterval = ((Number) faftConfig.getOrDefault("verify-interval", 10000)).intValue();
            verifyDuration = ((Number) faftConfig.getOrDefault("verify-duration", 1000)).intValue();
        }
        conf.put("faft.verify.interval", verifyInterval);
        conf.put("faft.verify.duration", verifyDuration);

        Map<String, List<String>> dag = new HashMap<>();
        dag.put("source-spout", new ArrayList<>(List.of("event-filter-bolt")));
        dag.put("event-filter-bolt", new ArrayList<>(List.of("project-bolt")));
        dag.put("project-bolt", new ArrayList<>(List.of("split-bolt")));
        dag.put("split-bolt", new ArrayList<>(List.of("chaos-bolt")));
        dag.put("chaos-bolt", new ArrayList<>(List.of("join-bolt")));
        dag.put("join-bolt", new ArrayList<>(List.of("window-agg-bolt")));
        dag.put("window-agg-bolt", new ArrayList<>(List.of("faft-sink-bolt")));
        dag.put("faft-sink-bolt", new ArrayList<>());

        List<String> sinkList = new ArrayList<>(List.of("faft-sink-bolt"));

        conf.put("faft.dag", dag);
        conf.put("faft.sinks", sinkList);

        Map<String, OperatorInfo> infos = new HashMap<>();
        infos.put("event-filter-bolt", new OperatorInfo("event-filter-bolt", 0.20, 0.10, 0.20));
        infos.put("project-bolt", new OperatorInfo("project-bolt", 0.20, 0.15, 0.25));
        infos.put("split-bolt", new OperatorInfo("split-bolt", 0.20, 0.20, 0.30));
        infos.put("chaos-bolt", new OperatorInfo("chaos-bolt", 0.10, 0.10, 0.15));
        infos.put("join-bolt", new OperatorInfo("join-bolt", 0.50, 0.40, 0.60));
        infos.put("window-agg-bolt", new OperatorInfo("window-agg-bolt", 0.70, 0.60, 0.90));
        infos.put("faft-sink-bolt", new OperatorInfo("faft-sink-bolt", 0.10, 0.10, 0.20));

        NodeImportanceEvaluator.Weights defaultWeightsObj = new NodeImportanceEvaluator.Weights(defAlpha, defBeta,
                defGamma);

        Map<String, NodeImportanceEvaluator.Weights> weightsObjMap = new HashMap<>();
        for (Map.Entry<String, List<Double>> entry : rawWeightsMap.entrySet()) {
            List<Double> val = entry.getValue();
            if (val != null && val.size() >= 3) {
                weightsObjMap.put(entry.getKey(),
                        new NodeImportanceEvaluator.Weights(val.get(0), val.get(1), val.get(2)));
            }
        }

        HashSet<String> sinks = new HashSet<>(sinkList);
        NodeImportanceEvaluator.Result res = NodeImportanceEvaluator.evaluateAndAssignRatios(
                dag, sinks, infos,
                weightsObjMap, defaultWeightsObj,
                impactDelta, omegaIn, omegaOut,
                rmin, rmax);

        Map<String, Double> ratios = res.R;
        System.out.println("[YSB Init] 初始采样率 ratios = " + ratios);
        conf.put("faft.ratios", ratios);

        Map<String, String> importanceStr = new HashMap<>();
        for (Map.Entry<String, Double> e : res.I.entrySet()) {
            importanceStr.put(e.getKey(), String.valueOf(e.getValue()));
        }
        conf.put("faft.importance", importanceStr);
        System.out.println("[YSB Init] 初始重要性 importance =" + importanceStr);

        if (args != null && args.length > 0) {
            String topologyName = args[0];
            try {
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Wait for local cluster...");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ysb-topology-local", conf, builder.createTopology());
            System.out.println("YSB 实验已启动！正在运行中...");
            System.out.println("程序正处于死循环保活状态。如需结束，请手动点击 IDEA 红色停止按钮。");
            while (true) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }

    private static double getDouble(Map<String, Object> map, String key, double defaultValue) {
        Object val = map.get(key);
        return toDouble(String.valueOf(val), defaultValue);
    }
}
