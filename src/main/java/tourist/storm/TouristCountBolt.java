package tourist.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 汇总并输出游客数
 */
public class TouristCountBolt extends BaseRichBolt {
    private AtomicInteger count = new AtomicInteger();


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    }

    @Override
    public void execute(Tuple tuple) {
        int delta = tuple.getInteger(0);
        System.out.println(count.addAndGet(delta));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
