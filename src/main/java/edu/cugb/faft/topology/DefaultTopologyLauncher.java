package edu.cugb.faft.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class DefaultTopologyLauncher {
    public static void main(String[] args) throws Exception {
        // 1. 构建拓扑
        TopologyBuilder builder = new TopologyBuilder();

        // 数据源 Spout
        builder.setSpout("source-spout", new SourceSpout(), 1);

        // 拆分句子 -> 单词
        builder.setBolt("split-bolt", new SplitBolt(), 2)
                .shuffleGrouping("source-spout");

        // 过滤停用词
        builder.setBolt("filter-bolt", new FilterBolt(), 2)
                .shuffleGrouping("split-bolt");

        // 故障注入算子。 5% 概率 fail，10% 概率延迟100ms
        builder.setBolt("chaos-bolt", new ChaosBolt(0.05, 0.05, 100), 2)
                .shuffleGrouping("filter-bolt");

        // 普通计数器（没有近似容错）
        builder.setBolt("count-bolt", new CountBolt(), 2)
                .fieldsGrouping("chaos-bolt", new org.apache.storm.tuple.Fields("word"));

        // 普通下沉（直接输出）
        builder.setBolt("sink-bolt", new SinkBolt(), 1)
                .globalGrouping("count-bolt");

        // 2. 配置
        Config conf = new Config();
        conf.setDebug(false); // 关闭 debug，避免日志过多
        conf.setNumWorkers(2); // Worker 数量，集群上用
        conf.setMessageTimeoutSecs(5); // 默认重试是 30s，这里调小，方便测试

        // 提高统计刷新率，方便 UI 观察，加速发射
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 1.0);

        // 3. 根据运行模式提交
        if (args != null && args.length > 0) {
            // 集群模式
            String topologyName = args[0];
            try {
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else {
            // 本地模式，方便调试
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("default-topology-local", conf, builder.createTopology());
            Thread.sleep(30000); // 跑 30 秒
            cluster.killTopology("default-topology-local");
            cluster.shutdown();
        }
    }
}
