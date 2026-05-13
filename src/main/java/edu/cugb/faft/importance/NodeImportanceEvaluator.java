package edu.cugb.faft.importance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 计算 I(v) = α*O(v) + β*D(v) + γ*C(v)
 * 并按 I(v) 归一映射到初始采样率 r(v) ∈ [rmin, rmax]
 */
public class NodeImportanceEvaluator {

    /**
     * 权重封装类，用于传递 alpha, beta, gamma
     */
    public static class Weights {
        public final double alpha, beta, gamma;
        public Weights(double alpha, double beta, double gamma) {
            this.alpha = alpha;
            this.beta = beta;
            this.gamma = gamma;
        }
    }

    /**
     * 评估结果封装类
     */
    public static class Result {
        public final Map<String, Double> O, D, C; // 3个子指标(已归一化)
        public final Map<String, Double> I;  // 算子重要性
        public final Map<String, Double> R;  // 初始采样率
        public final double maxI;

        public Result(Map<String, Double> O, Map<String, Double> D, Map<String, Double> C,
                      Map<String, Double> I, Map<String, Double> R, double maxI) {
            this.O = O;
            this.D = D;
            this.C = C;
            this.I = I;
            this.R = R;
            this.maxI = maxI;
        }
    }

    /**
     * 执行评估并分配采样率
     *
     * @param dag            有向无环图结构
     * @param sinks          Sink 节点集合
     * @param opInfo         算子运行时信息 (CPU/Mem/TPS)
     * @param weightsMap     差异化权重配置表 (Key: componentId, Value: Weights)
     * @param defaultWeights 默认权重配置 (当 Map 中找不到时使用)
     * @param delta          O(v) 计算用的衰减系数
     * @param omegaIn        D(v) 入度敏感度权重
     * @param omegaOut       D(v) 出度敏感度权重
     * @param rmin           最小采样率
     * @param rmax           最大采样率
     * @return 评估结果 Result
     */
    public static Result evaluateAndAssignRatios(
            Map<String, List<String>> dag,              // DAG
            Set<String> sinks,                          // Sink 集合
            Map<String, OperatorInfo> opInfo,           // 算子资源/吞吐信息
            Map<String, Weights> weightsMap,            // 差异化权重表
            Weights defaultWeights,                     // 默认权重
            double delta,                               // O(v) 衰减参数
            double omegaIn, double omegaOut,            // D(v) 入度/出度权重
            double rmin, double rmax)                   // 采样率上下限
    {

        // 1) O(v)
        Map<String, Double> O = OutputImpact.compute(dag, sinks, delta);
        O = normalize(O);

        // 2) D(v)
        Map<String, Double> D = UpDownStreamDependency.compute(dag, omegaIn, omegaOut);
        D = normalize(D);

        // 3) C(v)（未归一）→ 归一（若 opInfo 缺失则默认 0）
        Map<String, Double> Craw = new HashMap<>();
        for (String v : dag.keySet()) {
            OperatorInfo oi = opInfo.get(v);
            double c = (oi == null) ? 0.0 : ComputeComplexity.compute(oi.cpuUsage, oi.memUsage, oi.tps,
                    0.5, 0.3, 0.2);
            Craw.put(v, c);
        }
        Map<String, Double> C = normalize(Craw);


        // 4) I(v) 针对每个节点，取其专属权重系数
        Map<String, Double> I = new HashMap<>();
        double maxI = 0.0;
        // 确保 weightsMap 不为 null，防空指针
        if (weightsMap == null) weightsMap = new HashMap<>();

        for (String v : dag.keySet()) {
            // 获取该算子的权重配置，如果没有则使用默认值
            Weights w = weightsMap.getOrDefault(v, defaultWeights);

            double iv = w.alpha * O.getOrDefault(v, 0.0)
                    + w.beta  * D.getOrDefault(v, 0.0)
                    + w.gamma * C.getOrDefault(v, 0.0);

            I.put(v, iv);
            maxI = Math.max(maxI, iv);
        }

        // 5) r(v)
        // 线性映射: r(v) = rmin + (I(v) / maxI) * (rmax - rmin)
        Map<String, Double> R = new HashMap<>();
        for (String v : dag.keySet()) {
            double curI = I.get(v);
            double rv;
            if (maxI <= 1e-9) {
                // 避免除以 0，若所有节点重要性都为 0，则给最小采样率
                rv = rmin;
            } else {
                rv = rmin + (curI / maxI) * (rmax - rmin);
            }
            R.put(v, rv);
        }

        return new Result(O, D, C, I, R, maxI);
    }

    private static Map<String, Double> normalize(Map<String, Double> m) {
        double max = 0.0;
        for (double v : m.values()) max = Math.max(max, v);
        // 如果最大值为0，直接返回原 Map (全是0)
        if (max <= 1e-9) return new HashMap<>(m);
        Map<String, Double> out = new HashMap<>();
        for (Map.Entry<String, Double> e : m.entrySet()) out.put(e.getKey(), e.getValue() / max);
        return out;
    }
}
