package stormdigester.storm;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 *
 */
public class TouristTopology {
    private static final long ONE_HOUR = 60 * 60 * 1000;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("signaling", new SignalingSpout());
        builder.setBolt("worker", new WorkerDetectorBolt(
                new WorkerDetectorBolt.Metrics(8 * ONE_HOUR, 18 * ONE_HOUR, 3 * ONE_HOUR),
                new WorkerDetectorBolt.Metrics(18 * ONE_HOUR, 8 * ONE_HOUR, 3 * ONE_HOUR)
        )).fieldsGrouping("signaling", new Fields("imsi"))
                .noneGrouping("updateTime");
        builder.setBolt("tourist", new TouristBolt()).globalGrouping("delta");
    }
}
