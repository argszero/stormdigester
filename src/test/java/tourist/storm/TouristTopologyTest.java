package tourist.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * TouristTopology Tester.
 */
public class TouristTopologyTest {


    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {

    }

    /**
     * Method: main(String[] args)
     */
    @Test
    public void testMain() throws Exception {
        TopologyBuilder builder = TouristTopology.getTopologyBuilder();
        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("tourist-count", conf, builder.createTopology());

        Thread.sleep(4 * 1000);
        Sender sender = new Sender(5001);
        sender.send("tourist 1", "2013-01-04 08:00:00", "", "tourist");

        sender.send("worker 1", "2013-01-04 08:00:00", "", "tourist");
        sender.send("worker 1", "2013-01-04 11:01:00", "", "out");

        sender.send("worker 1", "2013-01-05 08:00:00", "", "tourist");
        sender.send("worker 1", "2013-01-05 11:02:00", "", "out");


        sender.send("worker 1", "2013-01-06 08:00:00", "", "tourist");
        sender.send("worker 1", "2013-01-06 11:01:00", "", "out");

        sender.send("worker 1", "2013-01-07 08:00:00", "", "tourist");
        sender.send("worker 1", "2013-01-07 11:01:00", "", "out");

        sender.send("worker 1", "2013-01-08 08:00:00", "", "tourist");
        sender.send("worker 1", "2013-01-08 11:01:00", "", "out");

        sender.send("worker 1", "2013-01-09 08:00:00", "", "tourist");
        sender.send("worker 1", "2013-01-09 11:01:00", "", "out");


        Thread.sleep(1 * 1000);
        sender.close();
        Thread.sleep(1 * 1000);
        cluster.shutdown();
    }
    @Test
    public void testMain2() throws Exception {
        TopologyBuilder builder = TouristTopology.getTopologyBuilder();
        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("tourist-count", conf, builder.createTopology());

        Thread.sleep(4 * 1000);
        Sender sender = new Sender(5001);
        sender.send("100001019584781", 1356969616945L, "dc" , "mall");
        sender.send("100001019584781", 1356969613957L, "dc" , "company");

        Thread.sleep(1 * 1000);
        sender.close();
        Thread.sleep(1 * 1000);
        cluster.shutdown();
    }

    private static class Sender extends IoHandlerAdapter {
        private final IoConnector connector;
        private final IoSession session;

        private Sender(int port) {
            connector = new NioSocketConnector();
            connector.setHandler(this);
            connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(new TextLineCodecFactory(Charset.forName("UTF-8"))));
            ConnectFuture future = connector.connect(new InetSocketAddress(port));
            future.awaitUninterruptibly();
            session = future.getSession();
        }

        public void send(String imsi, String time, String loc, String cell) throws ParseException {
            StringBuilder sb = new StringBuilder();
            session.write(sb.append(imsi).append(",").append(getTime(time)).append(",").append(loc).append(",").append(cell));
        }
        public void send(String imsi, long time, String loc, String cell) throws ParseException {
            StringBuilder sb = new StringBuilder();
            session.write(sb.append(imsi).append(",").append(time).append(",").append(loc).append(",").append(cell));
        }

        private long getTime(String s) throws ParseException {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").parse(s + " +0000").getTime();
        }

        public void close() {
            connector.dispose();
        }
    }

} 
