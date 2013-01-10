package tourist.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tourist.util.NioServer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.String.format;

/**
 *
 */
public class SignalingSpout extends BaseRichSpout implements NioServer.Listener {
    public static final String SIGNALING = "signaling";
    private static Logger logger = LoggerFactory.getLogger(SignalingSpout.class);
    LinkedBlockingQueue<String> queue = null;
    NioServer nioServer = null;
    private SpoutOutputCollector spoutOutputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(SIGNALING, new Fields("imsi", "time", "loc", "cell"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        queue = new LinkedBlockingQueue<String>(1000);
        nioServer = new NioServer(5001, this);
        try {
            nioServer.start();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void nextTuple() {
        String message = queue.poll();
        if (message != null) {
            String[] columns = message.split(",");
            Values tuple = new Values(columns[0], Long.parseLong(columns[1]), columns[2], columns[3]);
            if (logger.isInfoEnabled()) {
                logger.info(format("[%s]:%s", SIGNALING, tuple.toString()));
            }
            spoutOutputCollector.emit(SIGNALING, tuple);
        }
    }

    @Override
    public void messageReceived(String message) throws Exception {
        queue.offer(message);
    }

    @Override
    public void close() {
        try {
            nioServer.stop();
        } catch (IOException e) {
            logger.warn("error when stop nioServer", e);
        }
    }
}
