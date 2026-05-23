package edu.cugb.faft.topology;

import edu.cugb.faft.manager.ApproxBackupManager;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class EventFilterBolt extends BaseBasicBolt {

    private transient ApproxBackupManager backupManager;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        try {
            this.backupManager = ApproxBackupManager.getInstance();
        } catch (Exception e) {
            this.backupManager = null;
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        long t0 = System.nanoTime();
        try {
            String line = input.getStringByField("sentence");
            if (line != null && !line.trim().isEmpty() && line.contains(",")) {
                collector.emit(new Values(line));
            }
        } finally {
            if (backupManager != null) {
                backupManager.reportExecuteNanos("event-filter-bolt", System.nanoTime() - t0);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
