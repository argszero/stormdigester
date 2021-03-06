package tourist2.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 *
 */
public class TouristTopology {
//  private static final long ONE_HOUR = 60 * 60 * 1000;

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        TopologyBuilder builder = getTopologyBuilder();
        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) { // 远程模式
            System.out.println("Remote mode");
            conf.setNumWorkers(6);
            conf.setMaxSpoutPending(100);
            conf.setNumAckers(10);
            conf.setMessageTimeoutSecs(5);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            // 本地模式，调试代码
            System.out.println("Local mode");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("touristTopology", conf, builder.createTopology());

            Utils.sleep(600000);
            cluster.shutdown();
        }
    }

    public static TopologyBuilder getTopologyBuilder() {
        TopologyBuilder builder = new TopologyBuilder();

        String signalingSpout1 = "signalingSpout1";
//        String signalingSpout2 = "signalingSpout2";
        String userGroupStatusDetectorBolt = "userGroupStatusDetectorBolt";
        String touristCountBolt = "touristCountBolt";

        builder.setSpout(signalingSpout1, new SignalingSpout(5001));
//        builder.setSpout(signalingSpout2, new SignalingSpout(5002));

        builder.setBolt(userGroupStatusDetectorBolt, new UserGroupStatusDetectorBolt(), 4)
                .fieldsGrouping(signalingSpout1, SignalingSpout.SIGNALING, new Fields("imsi"))
                .allGrouping(signalingSpout1, SignalingSpout.TIME);
//                .fieldsGrouping(signalingSpout2, SignalingSpout.SIGNALING, new Fields("imsi"))
//                .allGrouping(signalingSpout2, SignalingSpout.TIME);
        builder.setBolt(touristCountBolt, new TouristCountBolt(), 1)
                .globalGrouping(userGroupStatusDetectorBolt, UserGroupStatusDetectorBolt.DETECTORSTREAM);
        return builder;
    }
}
