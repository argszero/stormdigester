package tourist2.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tourist2.util.Accout;
import tourist2.util.UserGroup;

import java.util.Map;

/**
 * 接收所有的信令
 */
public class UserGroupStatusDetectorBolt extends BaseBasicBolt implements UserGroup.Listener{
    private UserGroup userGroup = new UserGroup(this);
//    private OutputCollector collector;
    private BasicOutputCollector collector;
    public static final String USERGROUPSTATUSDETECTORSTREAM = "userGroupStatusDetectorStream";
//    private ThreadLocal<Tuple> tuple = new ThreadLocal<Tuple>();

//    @Override
//    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
//        this.collector = collector;
//    }

//    @Override
//    public void execute(Tuple input) {
////        this.tuple.set(input);
//        String sourceStreamId = input.getSourceStreamId();
//        if (SignalingSpout.SIGNALING.equals(sourceStreamId)) {
//            String imsi = input.getString(0);
//            long time = input.getLong(1);
//            String loc = input.getString(2);
//            String cell = input.getString(3);
//            userGroup.onSignal(time, imsi, loc, cell);
//        } else if (SignalingSpout.TIME.equals(sourceStreamId)) {
//            long signalTime = input.getLong(0);
//            userGroup.updateGlobleTime(input.getLong(0));
//        }
//        this.collector.ack(input);
//    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        this.collector = collector;
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
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(USERGROUPSTATUSDETECTORSTREAM, new Fields("time", "imsi", "status")); //用户有三种状态:1：游客(tourist),2:工作人员(worker),3:什么都不是(normal)
    }

    @Override
    public void onAddTourist(long userTime, String imsi, Accout.Status preStatus) {
        if (preStatus == Accout.Status.Tourist) return;
        this.collector.emit(USERGROUPSTATUSDETECTORSTREAM, new Values(userTime, imsi, "tourist"));
        System.out.println(String.format("+t:%s %s", imsi, userTime));
    }

    @Override
    public void onAddWorker(long userTime, String imsi, Accout.Status preStatus) {
        if (preStatus == Accout.Status.Worker) return;
        this.collector.emit(USERGROUPSTATUSDETECTORSTREAM, new Values(userTime, imsi, "worker"));
        System.out.println(String.format("-w:%s %s", imsi, userTime));
    }

    @Override
    public void onAddNormal(long userTime, String imsi, Accout.Status preStatus) {
        if (preStatus == Accout.Status.Normal) return;
        this.collector.emit(USERGROUPSTATUSDETECTORSTREAM, new Values(userTime, imsi, "normal"));
        System.out.println(String.format("-n:%s %s", imsi, userTime));
    }
}
