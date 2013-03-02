package tourist.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import tourist.util.MetricsDetector;

/**
 *
 */
public class TouristTopology {
    private static final long ONE_HOUR = 60 * 60 * 1000;

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        TopologyBuilder builder = getTopologyBuilder();
        Config conf = new Config();
//        conf.setDebug(true);
        conf.setNumWorkers(10);
        conf.setMaxSpoutPending(1000);
        conf.setNumAckers(30);
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }

    public static TopologyBuilder getTopologyBuilder() {
        TopologyBuilder builder = new TopologyBuilder();
        String signalingSpout = "signalingSpout";
        String updateTimeBolt = "updateTimeBolt";
        builder.setSpout(signalingSpout, new SignalingSpout());
        builder.setBolt(updateTimeBolt, new UpdateTimeBolt(), 1).globalGrouping(signalingSpout, SignalingSpout.SIGNALING);
//        builder.setBolt(updateTimeBolt, new UpdateTimeBolt(), 4).fieldsGrouping(signalingSpout, SignalingSpout.SIGNALING, new Fields("imsi"));
//        builder.setBolt(updateTimeBolt, new UpdateTimeBolt(), 1).allGrouping(signalingSpout, SignalingSpout.SIGNALING);
        builder.setBolt("touristCountChangeBolt", new TouristCountChangeBolt(
                new MetricsDetector.Metrics(8 * ONE_HOUR, 18 * ONE_HOUR, 3 * ONE_HOUR, 5),
                new MetricsDetector.Metrics(18 * ONE_HOUR, 8 * ONE_HOUR, 3 * ONE_HOUR, 5)
        ), 1).fieldsGrouping(signalingSpout, SignalingSpout.SIGNALING, new Fields("imsi"))
                .allGrouping(updateTimeBolt, UpdateTimeBolt.UPDATE_TIME);
        builder.setBolt("touristCountBolt", new TouristCountBolt(), 1).globalGrouping("touristCountChangeBolt");
        return builder;
    }
}
