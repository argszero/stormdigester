package tourist.storm;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import tourist.util.MetricsDetector;

/**
 *
 */
public class TouristTopology {
    private static final long ONE_HOUR = 60 * 60 * 1000;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("signaling", new SignalingSpout());
        builder.setBolt("touristCountChangeBolt", new TouristCountChangeBolt(
                new MetricsDetector.Metrics(8 * ONE_HOUR, 18 * ONE_HOUR, 3 * ONE_HOUR, 5),
                new MetricsDetector.Metrics(18 * ONE_HOUR, 8 * ONE_HOUR, 3 * ONE_HOUR, 5)
        )).fieldsGrouping("signaling", new Fields("imsi"))
                .noneGrouping("updateTime");
        builder.setBolt("tourist", new TouristCountChangeBolt()).globalGrouping("delta");
    }
}
