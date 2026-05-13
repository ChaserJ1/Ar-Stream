package edu.cugb.faft.importance;

import java.util.*;

/**
 * 局部结构依赖度 D(v)：
 * 基于算子在图中的入度 Din(v) 与出度 Dout(v)，通过线性加权计算其在拓扑中的枢纽地位。
 * D(v) = ωin * Din(v) + ωout * Dout(v)
 */
public class UpDownStreamDependency {

    /**
     * @param dag     有向无环图：op -> 下游列表
     * @param omegaIn 入度敏感度权重
     * @param omegaOut 出度敏感度权重
     */
    public static Map<String, Double> compute(Map<String, List<String>> dag, double omegaIn, double omegaOut) {
        Map<String, Double> D = new HashMap<>();

        Map<String, Integer> indegree = new HashMap<>();
        Map<String, Integer> outdegree = new HashMap<>();

        for (String v : dag.keySet()) {
            indegree.putIfAbsent(v, 0);
            outdegree.put(v, dag.getOrDefault(v, Collections.emptyList()).size());
        }

        for (List<String> outs : dag.values()) {
            for (String u : outs) {
                indegree.put(u, indegree.getOrDefault(u, 0) + 1);
            }
        }

        for (String v : dag.keySet()) {
            double score = omegaIn * indegree.getOrDefault(v, 0)
                    + omegaOut * outdegree.getOrDefault(v, 0);
            D.put(v, score);
        }
        return D;
    }
}
