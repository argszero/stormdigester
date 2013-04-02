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
  private static final long ONE_HOUR = 60 * 60 * 1000;

  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
    TopologyBuilder builder = getTopologyBuilder();
    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) { // 远程模式
      System.out.println("Remote mode");
      conf.setNumWorkers(10);
      conf.setMaxSpoutPending(100);
      conf.setNumAckers(4);
      conf.setMessageTimeoutSecs(5);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {
      // 本地模式，调试代码
      System.out.println("Local mode");
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("touristTopology", conf, builder.createTopology());

      Utils.sleep(60000);
      cluster.shutdown();
    }
  }

  public static TopologyBuilder getTopologyBuilder() {
    TopologyBuilder builder = new TopologyBuilder();
    String signalingSpout = "signalingSpout";
    String updateTimeBolt = "updateTimeBolt";
    builder.setSpout(signalingSpout, new tourist.storm.SignalingSpout());
    builder.setBolt(updateTimeBolt, new UserGroupStatusDetectorBolt(), 10)
        .fieldsGrouping(signalingSpout, SignalingSpout.SIGNALING, new Fields("imsi"))
        .globalGrouping(signalingSpout, SignalingSpout.TIME);
    return builder;
  }
}
