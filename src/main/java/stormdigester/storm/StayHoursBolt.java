package stormdigester.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.map.LazyMap;
import stormdigester.DaysStayTimeDetector;
import stormdigester.KbUtils;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class StayHoursBolt extends BaseRichBolt {
    private final long startOfDay;
    private final long endOfDay;
    private final long stayTimeThreshold;
    private long now;
    private final LazyMap detectors = (LazyMap) LazyMap.decorate(new HashMap(), new Transformer() {
        @Override
        public Object transform(final Object input) {
            return new DaysStayTimeDetector(startOfDay, endOfDay, stayTimeThreshold, new DaysStayTimeDetector.Listener() {
                @Override
                public void onChange(long startTime, long stayTime) {
                    StayHoursBolt.this.onChange((String) input, stayTime, startTime);
                }
            });
        }
    });
    private OutputCollector outputCollector;

    public StayHoursBolt(long startOfDay, long endOfDay, long stayTimeThreshold) {
        this.startOfDay = startOfDay;
        this.endOfDay = endOfDay;
        this.stayTimeThreshold = stayTimeThreshold;
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
            DaysStayTimeDetector detector = (DaysStayTimeDetector) detectors.get(imsi);
            if (KbUtils.getInstance().isInside(loc, cell)) {
                detector.in(time);
            } else {
                detector.out(time);
            }
            updateTime(detector, time);
        } else if (tuple.getSourceStreamId().equals("updateTime")) {
            long time = tuple.getLong(0);
            updateTime(null, time);
        }
    }

    private void updateTime(DaysStayTimeDetector exclude, long time) {
        if (time > now) {
            for (Object otherDetector : detectors.values()) {
                if (otherDetector != exclude) {
                    ((DaysStayTimeDetector) otherDetector).update(time);
                }
            }
        }
        now = time;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("imsi", "satisfy", "startTime"));
    }

    public void onChange(String input, long startTime, long stayTime) {
        this.outputCollector.emit("StayHoursBolt", new Values(input, stayTime > stayTimeThreshold, startTime));
    }
}
