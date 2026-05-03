package edu.cugb.faft.manager;

import edu.cugb.faft.importance.NodeImportanceEvaluator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 近似备份管理器（支持“按算子采样率 + 局部调节”）
 * - ratioByOperator：每算子基准采样率（来自启动时评估/动态重算）
 * - importanceMap  ：每算子重要性分数（来自评估器）
 * - adjustByError  ：当误差超阈值时，仅对“重要算子 Top P%”做上调；当误差远低于阈值时，对“低重要性算子 Bottom P%”做温和下调
 */
public class ApproxBackupManager implements Serializable {
    private static final long serialVersionUID = 1L;

    //单例模式
    private static volatile ApproxBackupManager INSTANCE;

    // Redis 连接池 (不可序列化，必须标记为 transient)
    private transient JedisPool jedisPool;

    // 资源开销埋点计数器 (原子类实现了 Serializable)
    private final AtomicLong ioCounter = new AtomicLong(0);

    // 配置与状态
    private final Random random = new Random();
    private volatile double currentRatio;  // 旧的全局比例（兼容老接口）
    private final double step;             // 调整步长（用于全局缩放/老接口）
    private final double rmin, rmax;

    // 每算子的“基准 r(v)”表（启动/重算时更新）
    private final ConcurrentHashMap<String, Double> ratioByOperator = new ConcurrentHashMap<>();

    // 每算子的“重要性分数”
    private final ConcurrentHashMap<String, Double> importanceMap = new ConcurrentHashMap<>();

    // 叠加在“基准 r”上的全局缩放，用于误差闭环（让所有算子一起收/放），已改为局部调节（留作备用）
    private volatile double globalScale = 1.0;

    // 统计：全局/每算子处理计数（TPS 差分用）
    private long processedCount = 0L, backupCount = 0L;
    private final ConcurrentHashMap<String, Long> processedByOp = new ConcurrentHashMap<>();

    // 动态重算循环
    private final AtomicBoolean rebalanceStarted = new AtomicBoolean(false);

    // 线程池 (不可序列化，transient，需在 readResolve 中重建或懒加载)
    private transient ScheduledExecutorService rebalanceExec;

    // 已完成，采用redis
/*    // 模拟远程状态存储 (KV Store) - 本地调试使用。 todo：后续上集群需修改至redis
    private final ConcurrentHashMap<String, Map<String, Integer>> remoteStateStore = new ConcurrentHashMap<>();*/

    // 放在初始化过程中了
/*    // 定期重算各个算子的重要性以及采样率
    private final ScheduledExecutorService rebalanceExec =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "faft-rebalancer");
                t.setDaemon(true);
                return t;
            });*/

    public static synchronized ApproxBackupManager init(double initRatio, double rmin, double rmax, double step) {
        if (INSTANCE != null) return INSTANCE;
        INSTANCE = new ApproxBackupManager(initRatio, rmin, rmax, step);
        return INSTANCE;
    }
    public static ApproxBackupManager getInstance() {
        if (INSTANCE == null) throw new IllegalStateException("ApproxBackupManager not initialized");
        return INSTANCE;
    }

    // 构造函数
    // 改为初始采样率为1，防止启动前期的误差积累
    private ApproxBackupManager(double initRatio, double rmin, double rmax, double step) {
        this.currentRatio = rmax;
        this.rmin = rmin;
        this.rmax = rmax;
        this.step = step;

        // 初始化 transient 字段
        initTransientFields();
    }

    // 专门用于初始化不可序列化的字段 (Redis, ThreadPool)
    private void initTransientFields() {
        // 1. 初始化 Redis 连接池
        if (this.jedisPool == null) {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(50);
            poolConfig.setMaxIdle(10);
            poolConfig.setTestOnBorrow(true);

            // 集群配置
            this.jedisPool = new JedisPool(poolConfig, "192.168.213.130", 6379);
            System.out.println("[FAFT] Redis Pool Initialized.");
        }

        // 2. 初始化重平衡线程池
        if (this.rebalanceExec == null) {
            this.rebalanceExec = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "faft-rebalancer");
                t.setDaemon(true);
                return t;
            });
        }
    }

    // Java 序列化钩子：反序列化时调用，确保 transient 字段被重新初始化
    private Object readResolve() {
        if (INSTANCE == null) {
            INSTANCE = this;
        }
        INSTANCE.initTransientFields();
        return INSTANCE;
    }

    // 用传来的采样率更新整张 per-op 采样率表
    public synchronized void updateSamplingRatios(Map<String, Double> ratios) {
        if (ratios == null || ratios.isEmpty()) return;
        ratioByOperator.clear();
        ratioByOperator.putAll(ratios);
        System.out.println("[FAFT RatioUpdate] " + ratioByOperator);
    }

    public synchronized void updateImportance(Map<String, Double> importance) {
        if (importance == null || importance.isEmpty()) return;
        importanceMap.clear();
        importanceMap.putAll(importance);
        System.out.println("[FAFT ImportanceUpdate] " + importanceMap);
    }

    /**
     * 尝试对指定算子进行备份操作
     *
     * @param operatorId 算子id，用于从表中查找对应的采样率
     * @param state 算子状态信息，当前未使用但保留用于后续扩展
     *//*
    public void tryBackup(String operatorId, Map<String, Integer> state) {
        // 更新采样计数器
        processedCount++;
        if (operatorId != null) processedByOp.merge(operatorId, 1L, Long::sum);

        // 根据算子id获取算子采样率
        double r = getRatioFor(operatorId);

        // 随机概率判断该算子是否需要做备份
        if (random.nextDouble() < r) {
            backupCount++;
            System.out.printf("[FAFT Backup][PER-OP] op=%s ratio=%.2f processed=%d backups=%d%n",
                    operatorId, r, processedCount, backupCount);
            // TODO: 真正的备份逻辑（落盘/远端等）
        }
    }*/

    /**
     * 具体的备份逻辑 (FaftCountBolt 调用) 写 redis
     * @param operatorId 算子ID
     * @param word       单词
     * @param count      计数值
     */
    public void tryBackup(String operatorId, int taskId, String word, int count) {
        processedCount++;
        if (operatorId != null) processedByOp.merge(operatorId, 1L, Long::sum);

        double r = getRatioFor(operatorId);

        // 采样判断
        if (random.nextDouble() < r) {
            backupCount++;

            // 埋点：I/O 计数 +1
            long currentIO = ioCounter.incrementAndGet();
            if (currentIO % 1000 == 0) {
                // [实验数据] 打印 I/O 开销
                System.out.println("[EXP-METRIC] Type=RESOURCE_IO_OPS TotalCount=" + currentIO + " TS=" + System.currentTimeMillis());
            }

            // 写 Redis (确保连接池已初始化)
            if (jedisPool == null) initTransientFields();
            try (Jedis jedis = jedisPool.getResource()) {
                // Key 格式: FAFT:BACKUP:{ComponentId}:{TaskId}
                String key = "FAFT:BACKUP:" + operatorId + ":" + taskId;
                jedis.hset(key, word, String.valueOf(count));
                jedis.expire(key, 3600); // 1小时过期
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 恢复备份 (用于故障恢复), 读 redis
     */
    public Map<String, Integer> getBackup(String operatorId, int taskId) {
        Map<String, Integer> result = new HashMap<>();
        String key = "FAFT:BACKUP:" + operatorId + ":" + taskId;

        if (jedisPool == null) initTransientFields();
        try (Jedis jedis = jedisPool.getResource()) {
            if (jedis.exists(key)) {
                Map<String, String> rawData = jedis.hgetAll(key);
                for (Map.Entry<String, String> entry : rawData.entrySet()) {
                    try {
                        result.put(entry.getKey(), Integer.parseInt(entry.getValue()));
                    } catch (NumberFormatException ignored) {}
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }


    // 根据算子 id，计算算子采样率 r
    private double getRatioFor(String operatorId) {
        // 从算子采样率基准表中获取算子基准采样率
        double base = ratioByOperator.getOrDefault(operatorId, currentRatio);
        // 乘以全局缩放因子
        return clamp(base * globalScale);
    }

    // 限制采样率范围
    private double clamp(double x) { return Math.max(rmin, Math.min(rmax, x)); }

    /**
     * 根据 Sink 端测得的误差进行反馈调节
     * @param Eobs 观测误差
     * @param Emax 允许的最大误差阈值
     */
    public synchronized void adjustByError(double Eobs, double Emax) {
        double lower = 0.5 * Emax;      // 滞回下界

        // 1. 获取所有算子条目，准备排序
        List<Map.Entry<String, Double>> sortedOps = new ArrayList<>(importanceMap.entrySet());

        // 2. 计算本轮目标调整多少个算子
        int targetCount = Math.max(1, (int) (sortedOps.size() * 0.3));
        int adjustedCount = 0;

        if (Eobs > Emax) { // 误差过大，执行上调

            // 计算动态步长
            double Kp = 2.5; // 比例系数
            double errorDiff = Eobs - Emax;
            double dynamicStep = (Kp * errorDiff) + this.step; // 动态步长

            // 打印 PID 触发日志
            System.out.printf("[FAFT-PID] 误差过高 (MRE=%.2f%%), 触发激进调节! 动态步长: %.3f (原步长: %.3f)%n",
                    Eobs * 100, dynamicStep, this.step);

            sortedOps.sort((a, b) -> Double.compare(b.getValue(), a.getValue())); // 降序排列
            int totalOps = sortedOps.size();

            // ========== 分层递进调节策略 ==========
            // Tier 1: Top 0%~30%  → 优先调整（收益最高）
            // Tier 2: 31%~60%     → Tier1 全满载后启用
            // Tier 3: 61%~100%    → Tier2 也全满载后启用
            int tier1End = Math.min(targetCount, totalOps);              // Top 30%
            int tier2End = Math.min((int)(totalOps * 0.6), totalOps);    // 31%~60%
            int tier3End = totalOps;                                     // 61%~100%

            int currentTier = 1;
            boolean allSaturatedInTier = true; // 当前层是否全部满载

            for (int i = 0; i < totalOps; i++) {
                Map.Entry<String, Double> entry = sortedOps.get(i);
                String op = entry.getKey();
                double oldR = ratioByOperator.getOrDefault(op, currentRatio);

                // 判断是否到达了层边界
                if (i == tier1End && currentTier == 1) {
                    if (!allSaturatedInTier) break; // Tier1 未全满载，不需要继续
                    // Tier1 全满载，升级到 Tier2
                    currentTier = 2;
                    allSaturatedInTier = true;
                    System.out.println("[FAFT Adjust] ⬆️ Tier1 (Top30%) 全部满载，升级至 Tier2 (31%-60%)");
                } else if (i == tier2End && currentTier == 2) {
                    if (!allSaturatedInTier) break; // Tier2 未全满载，不需要继续
                    // Tier2 也全满载，升级到 Tier3
                    currentTier = 3;
                    allSaturatedInTier = true;
                    System.out.println("[FAFT Adjust] ⬆️ Tier2 (31%-60%) 全部满载，升级至 Tier3 (61%-100%)");
                }

                // 如果该算子已经满载，跳过
                if (oldR >= rmax - 0.001) {
                    continue;
                }

                // 遇到了未满载的算子，标记当前层未全满载
                allSaturatedInTier = false;

                // 执行上调
                double newR = clamp(oldR + dynamicStep);
                ratioByOperator.put(op, newR);

                String tierTag = (currentTier == 1) ? "Tier1" : (currentTier == 2) ? "Tier2" : "Tier3";
                System.out.printf("[FAFT Adjust][%s-UP] op=%s oldR=%.3f newR=%.3f (error=%.4f > %.4f)%n",
                        tierTag, op, oldR, newR, Eobs, Emax);

                adjustedCount++;
            }

            if (adjustedCount == 0) {
                System.out.println("[FAFT Adjust] ⚠️ 警告：系统全员满载(1.0)，无法进一步降低误差！");
            } else if (currentTier > 1) {
                System.out.printf("[FAFT Adjust] ℹ️ 分层调节完成：升级至 Tier%d，本轮共调整了 %d 个算子%n",
                        currentTier, adjustedCount);
            }

        } else if (Eobs < lower) { // 误差过小，执行下调

            // 策略：按重要性从低到高排序 (优先牺牲不重要的节点)
            sortedOps.sort((a, b) -> Double.compare(a.getValue(), b.getValue())); // 升序

            for (Map.Entry<String, Double> entry : sortedOps) {
                String op = entry.getKey();
                double oldR = ratioByOperator.getOrDefault(op, currentRatio);

                // 如果该算子已经触底 (接近 0.1)，则跳过
                if (oldR <= rmin + 0.001) {
                    continue;
                }

                // 执行下调
                double newR = clamp(oldR - step);
                ratioByOperator.put(op, newR);

                System.out.printf("[FAFT Adjust][LOCAL-DOWN] op=%s oldR=%.3f newR=%.3f (error=%.4f < %.4f)%n",
                        op, oldR, newR, Eobs, lower);

                adjustedCount++;
                if (adjustedCount >= targetCount) break;
            }
        }
    }


    /**
     * 获得 Top/Bottom K 算子列表
     * @param highFirst 低分(false)/高分(true)优先
     * @param portion   选取比例
     * @return 算子列表
     */
    private List<String> topKByImportance(boolean highFirst, double portion) {
        List<Map.Entry<String, Double>> list = new ArrayList<>(importanceMap.entrySet());
        list.sort((a, b) -> highFirst
                ? Double.compare(b.getValue(), a.getValue())
                : Double.compare(a.getValue(), b.getValue()));
        int k = Math.max(1, (int) (list.size() * portion));
        List<String> out = new ArrayList<>();
        for (int i = 0; i < Math.min(k, list.size()); i++) {
            out.add(list.get(i).getKey());
        }
        return out;
    }

    // ====== Step4：动态重算 I→r（每 periodMs 刷新一次）====== 暂时为10s
    public void startDynamicRebalance(Map<String, List<String>> dag,
                                      Set<String> sinks,
                                      Map<String, NodeImportanceEvaluator.Weights> weightsMap,
                                      NodeImportanceEvaluator.Weights defaultWeights,
                                      double impactDelta, double decayAlpha,
                                      double rmin, double rmax,
                                      long periodMs) {
        if (!rebalanceStarted.compareAndSet(false, true)) return; // 避免重复启动

        final Map<String, Long> lastCount = new ConcurrentHashMap<>();
        rebalanceExec.scheduleAtFixedRate(() -> {
            try {
                // A) TPS 计算每算子的相对处理速度
                Map<String, Double> tps = new HashMap<>();
                for (String op : dag.keySet()) {
                    long now = processedByOp.getOrDefault(op, 0L);
                    long prev = lastCount.getOrDefault(op, 0L);
                    double rate = Math.max(0, now - prev) / (periodMs / 1000.0);
                    tps.put(op, rate);
                    lastCount.put(op, now);
                }
                double maxTps = tps.values().stream().mapToDouble(x -> x).max().orElse(1.0);

                // B) CPU / Mem 获取当前 JVM 进程的负载作为近似值
                double cpu = 0.0;
                try {
                    com.sun.management.OperatingSystemMXBean os =
                            (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
                    cpu = Math.max(0.0, os.getProcessCpuLoad()); // 0~1
                } catch (Throwable ignore) {}
                long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                double mem = (Runtime.getRuntime().maxMemory() > 0)
                        ? (used / (double) Runtime.getRuntime().maxMemory())
                        : 0.0;

                // C) 组装 OperatorInfo（TPS 归一化到 0~1）
                Map<String, edu.cugb.faft.importance.OperatorInfo> infos = new HashMap<>();
                for (String op : dag.keySet()) {
                    double ntps = maxTps == 0 ? 0 : tps.getOrDefault(op, 0.0) / maxTps;
                    infos.put(op, new edu.cugb.faft.importance.OperatorInfo(op, cpu, mem, ntps));
                }

                // D) 调用评估器重算 I→r
                NodeImportanceEvaluator.Result res =
                        NodeImportanceEvaluator.evaluateAndAssignRatios(
                                dag, sinks, infos, weightsMap, defaultWeights, impactDelta, decayAlpha, rmin, rmax);

                // E) 刷新 per-op 基准 r 表, 同步刷新 importance 表
                updateSamplingRatios(res.R);
                // System.out.println("[FAFT Rebalance] R=" + res.R);
                System.out.println("采样率已更新");
                updateImportance(res.I);
                System.out.println("重要性已更新");
                // System.out.println("[FAFT Rebalance] I=" + res.I);
            } catch (Throwable t) {
                t.printStackTrace();
                System.err.println("[FAFT Rebalance] failed: " + t.getMessage());
            }
        }, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    public void printStats() {
        System.out.printf("[FAFT Stats] Processed=%d | Backups=%d | globalScale=%.2f%n",
                processedCount, backupCount, globalScale);
    }

}
