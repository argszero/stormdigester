package tourist.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.lang.String.format;

public class UpdateTimeBolt extends BaseRichBolt {
    public static final String UPDATE_TIME = "updateTime";
    private static Logger logger = LoggerFactory.getLogger(UpdateTimeBolt.class);
    private long now;
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long time = tuple.getLong(1);
//        logger.info(format("[%s]:%s", UPDATE_TIME, tuple.toString()));
        if (now < time) {
            if (logger.isInfoEnabled()) {
//                logger.info(format("[%s]:%s", UPDATE_TIME, tuple.toString()));
            }
            this.outputCollector.emit(UPDATE_TIME, new Values(time));
            now = time;
        }
        this.outputCollector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(UPDATE_TIME, new Fields("time"));
    }
}
