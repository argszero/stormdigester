package tourist2.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tourist2.util.UserGroup;

import java.util.Map;

/**
 * 接收所有的信令
 */
public class UserGroupStatusDetectorBolt extends BaseRichBolt implements UserGroup.Listener {
  private UserGroup userGroup = new UserGroup(this);
  private OutputCollector collector;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    String sourceStreamId = input.getSourceStreamId();
    if (SignalingSpout.SIGNALING.equals(sourceStreamId)) {
      String imsi = input.getString(0);
      long time = input.getLong(1);
      String loc = input.getString(2);
      String cell = input.getString(3);
      userGroup.onSignal(time, imsi, loc, cell);
    } else if (SignalingSpout.TIME.equals(sourceStreamId)) {
      userGroup.updateGlobleTime(input.getLong(0));
    }
    collector.ack(input);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("time", "imsi", "status")); //用户有三种状态:1：游客(tourist),2:工作人员(worker),3:什么都不是(normal)
  }

  @Override
  public void onAddTourist(long userTime, String imsi) {
    collector.emit(new Values(userTime, imsi, "tourist"));
  }

  @Override
  public void onAddWorker(long userTime, String imsi) {
    collector.emit(new Values(userTime, imsi, "worker"));
  }

  @Override
  public void onAddNormal(long userTime, String imsi) {
    collector.emit(new Values(userTime, imsi, "normal"));
  }
}
