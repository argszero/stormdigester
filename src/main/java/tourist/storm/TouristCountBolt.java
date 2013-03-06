package tourist.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 汇总并输出游客数
 */
public class TouristCountBolt extends BaseRichBolt {
    private Logger countLogger = LoggerFactory.getLogger("tourist.count");
    private AtomicInteger count = new AtomicInteger();
    private OutputCollector outputCollector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        int delta = tuple.getInteger(0);
        count.addAndGet(delta);
        long time = tuple.getLong(1);
        countLogger.info(String.format("%s,%d", count.toString(), time));
        this.outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
