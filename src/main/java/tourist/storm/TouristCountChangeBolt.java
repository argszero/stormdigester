package tourist.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tourist.util.MetricsDetector;
import tourist.util.TouristDetector;

import java.util.Map;

/**
 * 当发现游客时，输出游客+1，当发现不是游客时，输出游客-1
 */
public class TouristCountChangeBolt extends BaseRichBolt implements TouristDetector.Listener {
    private Logger logger = LoggerFactory.getLogger(TouristCountChangeBolt.class);
    private TouristDetector detector;
    private OutputCollector outputCollector;

    public TouristCountChangeBolt(MetricsDetector.Metrics... metricses) {
        detector = new TouristDetector(this, metricses);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals("signaling")) {
            String imsi = tuple.getString(0);
            long time = tuple.getLong(1);
            String loc = tuple.getString(1);
            String cell = tuple.getString(2);
            detector.onSignaling(imsi, time, loc, cell);
        } else if (tuple.getSourceStreamId().equals("updateTime")) {
            long time = tuple.getLong(0);
            detector.updateTime(time);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void addTourist(String imsi) {
        outputCollector.emit("touristCountChange", new Values(+1));
    }

    @Override
    public void removeTourist(String imsi) {
        outputCollector.emit("touristCountChange", new Values(-1));
    }
}
