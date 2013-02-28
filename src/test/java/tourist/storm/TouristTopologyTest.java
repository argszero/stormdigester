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
import java.util.Date;
import java.util.TimeZone;

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
        sender.send("100001019584781", "2013-01-01 00:43:35", "chp", "tourist");
        sender.send("100001019584781", "2013-01-01 00:52:49", "xc", "tourist");

        Thread.sleep(1 * 1000);
        sender.close();
        Thread.sleep(1 * 1000);
        cluster.shutdown();
    }

    @Test
    public void testMain3() throws Exception {
        TopologyBuilder builder = TouristTopology.getTopologyBuilder();
        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("tourist-count", conf, builder.createTopology());

        Thread.sleep(4 * 1000);
        Sender sender = new Sender(5001);
        String[] signals = new String[]{"100001000008870,1356971895691,chy,stadium,Tue Jan 01 00:38:15 CST 2013"
                , "100001000008870,1356972882783,hd,home,Tue Jan 01 00:54:42 CST 2013"
                , "100001000008870,1356974379900,hd,home,Tue Jan 01 01:19:39 CST 2013"
                , "100001000008870,1356974791522,shy,stadium,Tue Jan 01 01:26:31 CST 2013"
                , "100001000008870,1356978257416,dc,airport,Tue Jan 01 02:24:17 CST 2013"
                , "100001000008870,1356980273701,dc,mall,Tue Jan 01 02:57:53 CST 2013"
                , "100001000008870,1356982544790,xc,home,Tue Jan 01 03:35:44 CST 2013"
                , "100001000008870,1356983090863,hd,airport,Tue Jan 01 03:44:50 CST 2013"
                , "100001000008870,1356984262750,shy,stadium,Tue Jan 01 04:04:22 CST 2013"
                , "100001000008870,1356985620735,xc,airport,Tue Jan 01 04:27:00 CST 2013"
                , "100001000008870,1356987922404,chp,home,Tue Jan 01 05:05:22 CST 2013"
                , "100001000008870,1356989059999,dc,stadium,Tue Jan 01 05:24:19 CST 2013"
                , "100001000008870,1356991542023,xc,mall,Tue Jan 01 06:05:42 CST 2013"
                , "100001000008870,1356994767173,chp,home,Tue Jan 01 06:59:27 CST 2013"
                , "100001000008870,1356995983281,chy,mall,Tue Jan 01 07:19:43 CST 2013"
                , "100001000008870,1356995994054,dc,mall,Tue Jan 01 07:19:54 CST 2013"
                , "100001000008870,1357001015454,chp,tourist,Tue Jan 01 08:43:35 CST 2013"
                , "100001000008870,1357001569894,xc,tourist,Tue Jan 01 08:52:49 CST 2013"
                , "100001000008870,1357002249147,shy,tourist,Tue Jan 01 09:04:09 CST 2013"
                , "100001000008870,1357003779918,hd,tourist,Tue Jan 01 09:29:39 CST 2013"
                , "100001000008870,1357007160724,hd,tourist,Tue Jan 01 10:26:00 CST 2013"
                , "100001000008870,1357008025411,xc,tourist,Tue Jan 01 10:40:25 CST 2013"
                , "100001000008870,1357011622213,dc,tourist,Tue Jan 01 11:40:22 CST 2013"
                , "100001000008870,1357011683287,hd,tourist,Tue Jan 01 11:41:23 CST 2013"
                , "100001000008870,1357016228399,xc,tourist,Tue Jan 01 12:57:08 CST 2013"
                , "100001000008870,1357016240131,xc,tourist,Tue Jan 01 12:57:20 CST 2013"
                , "100001000008870,1357019042044,hd,tourist,Tue Jan 01 13:44:02 CST 2013"
                , "100001000008870,1357019658435,chy,tourist,Tue Jan 01 13:54:18 CST 2013"
                , "100001000008870,1357022809507,chp,tourist,Tue Jan 01 14:46:49 CST 2013"
                , "100001000008870,1357022839190,chy,tourist,Tue Jan 01 14:47:19 CST 2013"
                , "100001000008870,1357023686833,chp,tourist,Tue Jan 01 15:01:26 CST 2013"
                , "100001000008870,1357024824440,dc,tourist,Tue Jan 01 15:20:24 CST 2013"
                , "100001000008870,1357029510003,chp,tourist,Tue Jan 01 16:38:30 CST 2013"
                , "100001000008870,1357030403615,xc,tourist,Tue Jan 01 16:53:23 CST 2013"
                , "100001000008870,1357030878398,chy,tourist,Tue Jan 01 17:01:18 CST 2013"
                , "100001000008870,1357033102506,hd,tourist,Tue Jan 01 17:38:22 CST 2013"
                , "100001000008870,1357036168635,chy,tourist,Tue Jan 01 18:29:28 CST 2013"
                , "100001000008870,1357037667652,dc,tourist,Tue Jan 01 18:54:27 CST 2013"
                , "100001000008870,1357039753726,xc,tourist,Tue Jan 01 19:29:13 CST 2013"
                , "100001000008870,1357041567575,chy,tourist,Tue Jan 01 19:59:27 CST 2013"
                , "100001000008870,1357043417746,shy,tourist,Tue Jan 01 20:30:17 CST 2013"
                , "100001000008870,1357044545180,hd,tourist,Tue Jan 01 20:49:05 CST 2013"
                , "100001000008870,1357047164950,chy,tourist,Tue Jan 01 21:32:44 CST 2013"
                , "100001000008870,1357047746101,shy,tourist,Tue Jan 01 21:42:26 CST 2013"
                , "100001000008870,1357049306109,shy,tourist,Tue Jan 01 22:08:26 CST 2013"
                , "100001000008870,1357051174099,chy,tourist,Tue Jan 01 22:39:34 CST 2013"
                , "100001000008870,1357054653128,chp,tourist,Tue Jan 01 23:37:33 CST 2013"
                , "100001000008870,1357055483596,dc,tourist,Tue Jan 01 23:51:23 CST 2013"
                , "100001000008870,1357056620086,dc,tourist,Wed Jan 02 00:10:20 CST 2013"
                , "100001000008870,1357056708659,chy,tourist,Wed Jan 02 00:11:48 CST 2013"
                , "100001000008870,1357060849048,xc,tourist,Wed Jan 02 01:20:49 CST 2013"
                , "100001000008870,1357061760014,chy,tourist,Wed Jan 02 01:36:00 CST 2013"
                , "100001000008870,1357063860237,dc,tourist,Wed Jan 02 02:11:00 CST 2013"
                , "100001000008870,1357065341023,shy,tourist,Wed Jan 02 02:35:41 CST 2013"
                , "100001000008870,1357067046420,chp,tourist,Wed Jan 02 03:04:06 CST 2013"
                , "100001000008870,1357069372669,dc,tourist,Wed Jan 02 03:42:52 CST 2013"
                , "100001000008870,1357070610628,shy,tourist,Wed Jan 02 04:03:30 CST 2013"
                , "100001000008870,1357073224258,dc,tourist,Wed Jan 02 04:47:04 CST 2013"
                , "100001000008870,1357074392418,dc,tourist,Wed Jan 02 05:06:32 CST 2013"
                , "100001000008870,1357076464338,hd,tourist,Wed Jan 02 05:41:04 CST 2013"
                , "100001000008870,1357078287039,dc,tourist,Wed Jan 02 06:11:27 CST 2013"
                , "100001000008870,1357080233462,dc,tourist,Wed Jan 02 06:43:53 CST 2013"
                , "100001000008870,1357081927826,hd,tourist,Wed Jan 02 07:12:07 CST 2013"
                , "100001000008870,1357082522020,xc,tourist,Wed Jan 02 07:22:02 CST 2013"
                , "100001000008870,1357085217576,shy,airport,Wed Jan 02 08:06:57 CST 2013"
                , "100001000008870,1357087585271,chy,home,Wed Jan 02 08:46:25 CST 2013"
                , "100001000008870,1357088707827,dc,home,Wed Jan 02 09:05:07 CST 2013"
                , "100001000008870,1357090052029,hd,airport,Wed Jan 02 09:27:32 CST 2013"
                , "100001000008870,1357094163740,xc,home,Wed Jan 02 10:36:03 CST 2013"
                , "100001000008870,1357094869767,shy,stadium,Wed Jan 02 10:47:49 CST 2013"
                , "100001000008870,1357095764046,dc,stadium,Wed Jan 02 11:02:44 CST 2013"
                , "100001000008870,1357098651996,chp,company,Wed Jan 02 11:50:51 CST 2013"
                , "100001000008870,1357099856024,chy,company,Wed Jan 02 12:10:56 CST 2013"
                , "100001000008870,1357100108055,shy,stadium,Wed Jan 02 12:15:08 CST 2013"
                , "100001000008870,1357104381169,chy,airport,Wed Jan 02 13:26:21 CST 2013"
                , "100001000008870,1357104809817,chy,mall,Wed Jan 02 13:33:29 CST 2013"
                , "100001000008870,1357108490451,hd,airport,Wed Jan 02 14:34:50 CST 2013"
                , "100001000008870,1357109741052,chp,stadium,Wed Jan 02 14:55:41 CST 2013"
                , "100001000008870,1357112824374,chy,home,Wed Jan 02 15:47:04 CST 2013"
                , "100001000008870,1357113517974,shy,home,Wed Jan 02 15:58:37 CST 2013"
                , "100001000008870,1357113885900,xc,mall,Wed Jan 02 16:04:45 CST 2013"
                , "100001000008870,1357113940924,chy,company,Wed Jan 02 16:05:40 CST 2013"
                , "100001000008870,1357118103399,xc,company,Wed Jan 02 17:15:03 CST 2013"
                , "100001000008870,1357120185036,shy,airport,Wed Jan 02 17:49:45 CST 2013"
                , "100001000008870,1357123389336,chy,mall,Wed Jan 02 18:43:09 CST 2013"
                , "100001000008870,1357124158161,dc,airport,Wed Jan 02 18:55:58 CST 2013"
                , "100001000008870,1357127052398,dc,mall,Wed Jan 02 19:44:12 CST 2013"
                , "100001000008870,1357127251790,chp,stadium,Wed Jan 02 19:47:31 CST 2013"
                , "100001000008870,1357129124107,chy,home,Wed Jan 02 20:18:44 CST 2013"
                , "100001000008870,1357131261906,dc,company,Wed Jan 02 20:54:21 CST 2013"
                , "100001000008870,1357132719952,shy,mall,Wed Jan 02 21:18:39 CST 2013"
                , "100001000008870,1357134446022,chp,mall,Wed Jan 02 21:47:26 CST 2013"
                , "100001000008870,1357135840332,xc,company,Wed Jan 02 22:10:40 CST 2013"
                , "100001000008870,1357137138421,hd,stadium,Wed Jan 02 22:32:18 CST 2013"
                , "100001000008870,1357141405693,chp,stadium,Wed Jan 02 23:43:25 CST 2013"
                , "100001000008870,1357142229719,dc,home,Wed Jan 02 23:57:09 CST 2013"
                , "100001000008870,1357144847479,chy,mall,Thu Jan 03 00:40:47 CST 2013"
                , "100001000008870,1357145959746,hd,company,Thu Jan 03 00:59:19 CST 2013"
                , "100001000008870,1357147056330,chy,mall,Thu Jan 03 01:17:36 CST 2013"
                , "100001000008870,1357149442130,hd,mall,Thu Jan 03 01:57:22 CST 2013"
                , "100001000008870,1357149909075,dc,home,Thu Jan 03 02:05:09 CST 2013"
                , "100001000008870,1357151618798,chp,company,Thu Jan 03 02:33:38 CST 2013"
                , "100001000008870,1357153900836,xc,company,Thu Jan 03 03:11:40 CST 2013"
                , "100001000008870,1357154631939,shy,company,Thu Jan 03 03:23:51 CST 2013"
                , "100001000008870,1357159332686,hd,airport,Thu Jan 03 04:42:12 CST 2013"
                , "100001000008870,1357159520311,hd,company,Thu Jan 03 04:45:20 CST 2013"
                , "100001000008870,1357161090807,chy,mall,Thu Jan 03 05:11:30 CST 2013"
                , "100001000008870,1357161318001,dc,mall,Thu Jan 03 05:15:18 CST 2013"
                , "100001000008870,1357164908943,chp,company,Thu Jan 03 06:15:08 CST 2013"
                , "100001000008870,1357166713570,chy,company,Thu Jan 03 06:45:13 CST 2013"
                , "100001000008870,1357167698125,dc,home,Thu Jan 03 07:01:38 CST 2013"
                , "100001000008870,1357169115452,chy,home,Thu Jan 03 07:25:15 CST 2013"
                , "100001000008870,1357171400964,chy,tourist,Thu Jan 03 08:03:20 CST 2013"
                , "100001000008870,1357172705924,chy,tourist,Thu Jan 03 08:25:05 CST 2013"
                , "100001000008870,1357176675797,hd,tourist,Thu Jan 03 09:31:15 CST 2013"
                , "100001000008870,1357178263876,dc,tourist,Thu Jan 03 09:57:43 CST 2013"
                , "100001000008870,1357180773440,shy,tourist,Thu Jan 03 10:39:33 CST 2013"
                , "100001000008870,1357181182387,shy,tourist,Thu Jan 03 10:46:22 CST 2013"
                , "100001000008870,1357182044245,chp,tourist,Thu Jan 03 11:00:44 CST 2013"
                , "100001000008870,1357183956785,chp,tourist,Thu Jan 03 11:32:36 CST 2013"
                , "100001000008870,1357187235286,chy,tourist,Thu Jan 03 12:27:15 CST 2013"
                , "100001000008870,1357187642504,chp,tourist,Thu Jan 03 12:34:02 CST 2013"
                , "100001000008870,1357191504498,chp,tourist,Thu Jan 03 13:38:24 CST 2013"
                , "100001000008870,1357191852634,hd,tourist,Thu Jan 03 13:44:12 CST 2013"
                , "100001000008870,1357195537213,shy,tourist,Thu Jan 03 14:45:37 CST 2013"
                , "100001000008870,1357196293448,shy,tourist,Thu Jan 03 14:58:13 CST 2013"
                , "100001000008870,1357199396539,shy,tourist,Thu Jan 03 15:49:56 CST 2013"
                , "100001000008870,1357199571375,xc,tourist,Thu Jan 03 15:52:51 CST 2013"
                , "100001000008870,1357200740907,shy,tourist,Thu Jan 03 16:12:20 CST 2013"
                , "100001000008870,1357201696221,chp,tourist,Thu Jan 03 16:28:16 CST 2013"
                , "100001000008870,1357204813651,hd,tourist,Thu Jan 03 17:20:13 CST 2013"
                , "100001000008870,1357206368836,chy,tourist,Thu Jan 03 17:46:08 CST 2013"
                , "100001000008870,1357207340948,chy,tourist,Thu Jan 03 18:02:20 CST 2013"
                , "100001000008870,1357209471214,chp,tourist,Thu Jan 03 18:37:51 CST 2013"
                , "100001000008870,1357212333191,chy,tourist,Thu Jan 03 19:25:33 CST 2013"
                , "100001000008870,1357213437616,hd,tourist,Thu Jan 03 19:43:57 CST 2013"
                , "100001000008870,1357214802715,xc,tourist,Thu Jan 03 20:06:42 CST 2013"
                , "100001000008870,1357214941248,shy,tourist,Thu Jan 03 20:09:01 CST 2013"
                , "100001000008870,1357219902099,hd,tourist,Thu Jan 03 21:31:42 CST 2013"
                , "100001000008870,1357220273660,xc,tourist,Thu Jan 03 21:37:53 CST 2013"
                , "100001000008870,1357223736136,shy,tourist,Thu Jan 03 22:35:36 CST 2013"
                , "100001000008870,1357223949080,shy,tourist,Thu Jan 03 22:39:09 CST 2013"
                , "100001000008870,1357225707849,hd,tourist,Thu Jan 03 23:08:27 CST 2013"
                , "100001000008870,1357227938921,chp,tourist,Thu Jan 03 23:45:38 CST 2013"
                , "100001000008870,1357229603926,xc,tourist,Fri Jan 04 00:13:23 CST 2013"
                , "100001000008870,1357229799993,shy,tourist,Fri Jan 04 00:16:39 CST 2013"
                , "100001000008870,1357232662062,xc,tourist,Fri Jan 04 01:04:22 CST 2013"
                , "100001000008870,1357233712531,xc,tourist,Fri Jan 04 01:21:52 CST 2013"
                , "100001000008870,1357237676816,shy,tourist,Fri Jan 04 02:27:56 CST 2013"
                , "100001000008870,1357237834019,chy,tourist,Fri Jan 04 02:30:34 CST 2013"
                , "100001000008870,1357241386104,xc,tourist,Fri Jan 04 03:29:46 CST 2013"
                , "100001000008870,1357241689439,xc,tourist,Fri Jan 04 03:34:49 CST 2013"
                , "100001000008870,1357244887280,hd,tourist,Fri Jan 04 04:28:07 CST 2013"
                , "100001000008870,1357246589507,hd,tourist,Fri Jan 04 04:56:29 CST 2013"
                , "100001000008870,1357248282250,hd,tourist,Fri Jan 04 05:24:42 CST 2013"
                , "100001000008870,1357249849395,shy,tourist,Fri Jan 04 05:50:49 CST 2013"
                , "100001000008870,1357252069352,dc,tourist,Fri Jan 04 06:27:49 CST 2013"
                , "100001000008870,1357253809856,chp,tourist,Fri Jan 04 06:56:49 CST 2013"
                , "100001000008870,1357254226172,shy,tourist,Fri Jan 04 07:03:46 CST 2013"
                , "100001000008870,1357254715492,chp,tourist,Fri Jan 04 07:11:55 CST 2013"
                , "100001000008870,1357258905744,hd,tourist,Fri Jan 04 08:21:45 CST 2013"
                , "100001000008870,1357259999395,hd,tourist,Fri Jan 04 08:39:59 CST 2013"
                , "100001000008870,1357261379523,dc,tourist,Fri Jan 04 09:02:59 CST 2013"
                , "100001000008870,1357264248132,dc,tourist,Fri Jan 04 09:50:48 CST 2013"
                , "100001000008870,1357264916885,chp,tourist,Fri Jan 04 10:01:56 CST 2013"
                , "100001000008870,1357265944183,xc,tourist,Fri Jan 04 10:19:04 CST 2013"
                , "100001000008870,1357269040415,shy,tourist,Fri Jan 04 11:10:40 CST 2013"
                , "100001000008870,1357269837987,xc,tourist,Fri Jan 04 11:23:57 CST 2013"
                , "100001000008870,1357272384894,chy,tourist,Fri Jan 04 12:06:24 CST 2013"
                , "100001000008870,1357274164315,xc,tourist,Fri Jan 04 12:36:04 CST 2013"
                , "100001000008870,1357277116754,chp,tourist,Fri Jan 04 13:25:16 CST 2013"
                , "100001000008870,1357277454930,shy,tourist,Fri Jan 04 13:30:54 CST 2013"
                , "100001000008870,1357279477536,shy,tourist,Fri Jan 04 14:04:37 CST 2013"
                , "100001000008870,1357281071139,chy,tourist,Fri Jan 04 14:31:11 CST 2013"
                , "100001000008870,1357284505318,dc,tourist,Fri Jan 04 15:28:25 CST 2013"
                , "100001000008870,1357286361034,dc,tourist,Fri Jan 04 15:59:21 CST 2013"
                , "100001000008870,1357287362561,hd,tourist,Fri Jan 04 16:16:02 CST 2013"
                , "100001000008870,1357289182280,dc,tourist,Fri Jan 04 16:46:22 CST 2013"
                , "100001000008870,1357290743487,chy,tourist,Fri Jan 04 17:12:23 CST 2013"
                , "100001000008870,1357292510036,chp,tourist,Fri Jan 04 17:41:50 CST 2013"
                , "100001000008870,1357294311829,shy,tourist,Fri Jan 04 18:11:51 CST 2013"
                , "100001000008870,1357294998716,dc,tourist,Fri Jan 04 18:23:18 CST 2013"
                , "100001000008870,1357299411550,chy,tourist,Fri Jan 04 19:36:51 CST 2013"
                , "100001000008870,1357299450168,hd,tourist,Fri Jan 04 19:37:30 CST 2013"
                , "100001000008870,1357303143716,shy,tourist,Fri Jan 04 20:39:03 CST 2013"
                , "100001000008870,1357304209503,hd,tourist,Fri Jan 04 20:56:49 CST 2013"
                , "100001000008870,1357305585346,chp,tourist,Fri Jan 04 21:19:45 CST 2013"
                , "100001000008870,1357306557711,chp,tourist,Fri Jan 04 21:35:57 CST 2013"
                , "100001000008870,1357309439424,dc,tourist,Fri Jan 04 22:23:59 CST 2013"
                , "100001000008870,1357309480084,dc,tourist,Fri Jan 04 22:24:40 CST 2013"
                , "100001000008870,1357313122648,chy,tourist,Fri Jan 04 23:25:22 CST 2013"
                , "100001000008870,1357313845319,shy,tourist,Fri Jan 04 23:37:25 CST 2013"
                , "100001000008870,1357317293554,shy,tourist,Sat Jan 05 00:34:53 CST 2013"
                , "100001000008870,1357317719025,chp,tourist,Sat Jan 05 00:41:59 CST 2013"
                , "100001000008870,1357321548879,chp,tourist,Sat Jan 05 01:45:48 CST 2013"
                , "100001000008870,1357321738502,chy,tourist,Sat Jan 05 01:48:58 CST 2013"
                , "100001000008870,1357322559611,shy,tourist,Sat Jan 05 02:02:39 CST 2013"
                , "100001000008870,1357324545933,shy,tourist,Sat Jan 05 02:35:45 CST 2013"
                , "100001000008870,1357327853603,hd,tourist,Sat Jan 05 03:30:53 CST 2013"
                , "100001000008870,1357327894156,xc,tourist,Sat Jan 05 03:31:34 CST 2013"
                , "100001000008870,1357330063035,dc,tourist,Sat Jan 05 04:07:43 CST 2013"
                , "100001000008870,1357331453802,xc,tourist,Sat Jan 05 04:30:53 CST 2013"
                , "100001000008870,1357335101031,xc,tourist,Sat Jan 05 05:31:41 CST 2013"
                , "100001000008870,1357335208362,xc,tourist,Sat Jan 05 05:33:28 CST 2013"
                , "100001000008870,1357337499911,chp,tourist,Sat Jan 05 06:11:39 CST 2013"
                , "100001000008870,1357338947387,xc,tourist,Sat Jan 05 06:35:47 CST 2013"
                , "100001000008870,1357341213066,chp,tourist,Sat Jan 05 07:13:33 CST 2013"
                , "100001000008870,1357341324000,shy,tourist,Sat Jan 05 07:15:24 CST 2013"
                , "100001000008870,1357344064524,shy,tourist,Sat Jan 05 08:01:04 CST 2013"
                , "100001000008870,1357346529230,xc,tourist,Sat Jan 05 08:42:09 CST 2013"
                , "100001000008870,1357348147266,dc,tourist,Sat Jan 05 09:09:07 CST 2013"
                , "100001000008870,1357348762420,chy,tourist,Sat Jan 05 09:19:22 CST 2013"
                , "100001000008870,1357352216986,hd,tourist,Sat Jan 05 10:16:56 CST 2013"
                , "100001000008870,1357353427419,xc,tourist,Sat Jan 05 10:37:07 CST 2013"
                , "100001000008870,1357354928779,chp,tourist,Sat Jan 05 11:02:08 CST 2013"
                , "100001000008870,1357355351329,xc,tourist,Sat Jan 05 11:09:11 CST 2013"
                , "100001000008870,1357358639550,xc,tourist,Sat Jan 05 12:03:59 CST 2013"
                , "100001000008870,1357360446135,xc,tourist,Sat Jan 05 12:34:06 CST 2013"
                , "100001000008870,1357363898627,hd,tourist,Sat Jan 05 13:31:38 CST 2013"
                , "100001000008870,1357364754798,dc,tourist,Sat Jan 05 13:45:54 CST 2013"
                , "100001000008870,1357367521116,xc,tourist,Sat Jan 05 14:32:01 CST 2013"
                , "100001000008870,1357368537684,hd,tourist,Sat Jan 05 14:48:57 CST 2013"
                , "100001000008870,1357370026695,xc,tourist,Sat Jan 05 15:13:46 CST 2013"
                , "100001000008870,1357371826888,hd,tourist,Sat Jan 05 15:43:46 CST 2013"
                , "100001000008870,1357374628254,shy,tourist,Sat Jan 05 16:30:28 CST 2013"
                , "100001000008870,1357376145187,dc,tourist,Sat Jan 05 16:55:45 CST 2013"
                , "100001000008870,1357378854751,xc,tourist,Sat Jan 05 17:40:54 CST 2013"
                , "100001000008870,1357378873479,shy,tourist,Sat Jan 05 17:41:13 CST 2013"
                , "100001000008870,1357382278149,shy,tourist,Sat Jan 05 18:37:58 CST 2013"
                , "100001000008870,1357383127320,chy,tourist,Sat Jan 05 18:52:07 CST 2013"
                , "100001000008870,1357383690291,chy,tourist,Sat Jan 05 19:01:30 CST 2013"
                , "100001000008870,1357385650228,chy,tourist,Sat Jan 05 19:34:10 CST 2013"
                , "100001000008870,1357388881571,dc,tourist,Sat Jan 05 20:28:01 CST 2013"
                , "100001000008870,1357390244549,chp,tourist,Sat Jan 05 20:50:44 CST 2013"
                , "100001000008870,1357391451672,dc,tourist,Sat Jan 05 21:10:51 CST 2013"
                , "100001000008870,1357391622437,chp,tourist,Sat Jan 05 21:13:42 CST 2013"
                , "100001000008870,1357396839021,hd,tourist,Sat Jan 05 22:40:39 CST 2013"
                , "100001000008870,1357397406487,chy,tourist,Sat Jan 05 22:50:06 CST 2013"
                , "100001000008870,1357399356118,chp,tourist,Sat Jan 05 23:22:36 CST 2013"
                , "100001000008870,1357400374387,xc,tourist,Sat Jan 05 23:39:34 CST 2013"
                , "100001000008870,1357402254987,chp,tourist,Sun Jan 06 00:10:54 CST 2013"
                , "100001000008870,1357405117666,chp,tourist,Sun Jan 06 00:58:37 CST 2013"
                , "100001000008870,1357406085365,chp,tourist,Sun Jan 06 01:14:45 CST 2013"
                , "100001000008870,1357406861278,shy,tourist,Sun Jan 06 01:27:41 CST 2013"
                , "100001000008870,1357409156314,dc,tourist,Sun Jan 06 02:05:56 CST 2013"
                , "100001000008870,1357409187821,xc,tourist,Sun Jan 06 02:06:27 CST 2013"
                , "100001000008870,1357412896635,hd,tourist,Sun Jan 06 03:08:16 CST 2013"
                , "100001000008870,1357414993669,chp,tourist,Sun Jan 06 03:43:13 CST 2013"
                , "100001000008870,1357416201610,chp,tourist,Sun Jan 06 04:03:21 CST 2013"
                , "100001000008870,1357418963496,shy,tourist,Sun Jan 06 04:49:23 CST 2013"
                , "100001000008870,1357419636986,chp,tourist,Sun Jan 06 05:00:36 CST 2013"
                , "100001000008870,1357420906122,chy,tourist,Sun Jan 06 05:21:46 CST 2013"
                , "100001000008870,1357424869128,shy,tourist,Sun Jan 06 06:27:49 CST 2013"
                , "100001000008870,1357426614876,hd,tourist,Sun Jan 06 06:56:54 CST 2013"
                , "100001000008870,1357426831914,xc,tourist,Sun Jan 06 07:00:31 CST 2013"
                , "100001000008870,1357428559724,shy,tourist,Sun Jan 06 07:29:19 CST 2013"
                , "100001000008870,1357431004383,chp,tourist,Sun Jan 06 08:10:04 CST 2013"
                , "100001000008870,1357431600277,shy,tourist,Sun Jan 06 08:20:00 CST 2013"
                , "100001000008870,1357434286323,chy,tourist,Sun Jan 06 09:04:46 CST 2013"
                , "100001000008870,1357436672308,chp,tourist,Sun Jan 06 09:44:32 CST 2013"
                , "100001000008870,1357437959971,hd,tourist,Sun Jan 06 10:05:59 CST 2013"
                , "100001000008870,1357440769490,chp,tourist,Sun Jan 06 10:52:49 CST 2013"
                , "100001000008870,1357442304080,chy,tourist,Sun Jan 06 11:18:24 CST 2013"
                , "100001000008870,1357444056066,shy,tourist,Sun Jan 06 11:47:36 CST 2013"
                , "100001000008870,1357445043810,dc,tourist,Sun Jan 06 12:04:03 CST 2013"
                , "100001000008870,1357448394471,chy,tourist,Sun Jan 06 12:59:54 CST 2013"
                , "100001000008870,1357449091578,hd,tourist,Sun Jan 06 13:11:31 CST 2013"
                , "100001000008870,1357451113751,chy,tourist,Sun Jan 06 13:45:13 CST 2013"
                , "100001000008870,1357454550765,chy,tourist,Sun Jan 06 14:42:30 CST 2013"
                , "100001000008870,1357454658885,hd,tourist,Sun Jan 06 14:44:18 CST 2013"
                , "100001000008870,1357456729434,xc,tourist,Sun Jan 06 15:18:49 CST 2013"
                , "100001000008870,1357456998233,chp,tourist,Sun Jan 06 15:23:18 CST 2013"
                , "100001000008870,1357459844006,chy,tourist,Sun Jan 06 16:10:44 CST 2013"
                , "100001000008870,1357461300995,chy,tourist,Sun Jan 06 16:35:00 CST 2013"
                , "100001000008870,1357465293993,xc,tourist,Sun Jan 06 17:41:33 CST 2013"
                , "100001000008870,1357466158824,shy,tourist,Sun Jan 06 17:55:58 CST 2013"
                , "100001000008870,1357467679576,chp,tourist,Sun Jan 06 18:21:19 CST 2013"
                , "100001000008870,1357468089494,hd,tourist,Sun Jan 06 18:28:09 CST 2013"
                , "100001000008870,1357471426641,xc,tourist,Sun Jan 06 19:23:46 CST 2013"
                , "100001000008870,1357472358313,shy,tourist,Sun Jan 06 19:39:18 CST 2013"
                , "100001000008870,1357475012565,dc,tourist,Sun Jan 06 20:23:32 CST 2013"
                , "100001000008870,1357476385294,chp,tourist,Sun Jan 06 20:46:25 CST 2013"
                , "100001000008870,1357480489120,shy,tourist,Sun Jan 06 21:54:49 CST 2013"
                , "100001000008870,1357480493650,hd,tourist,Sun Jan 06 21:54:53 CST 2013"
                , "100001000008870,1357481573168,dc,tourist,Sun Jan 06 22:12:53 CST 2013"
                , "100001000008870,1357483157735,chp,tourist,Sun Jan 06 22:39:17 CST 2013"
                , "100001000008870,1357485803909,chp,tourist,Sun Jan 06 23:23:23 CST 2013"
                , "100001000008870,1357487522714,hd,tourist,Sun Jan 06 23:52:02 CST 2013"
                , "100001000008870,1357488520705,shy,tourist,Mon Jan 07 00:08:40 CST 2013"
                , "100001000008870,1357488849442,hd,tourist,Mon Jan 07 00:14:09 CST 2013"
                , "100001000008870,1357493693432,chp,tourist,Mon Jan 07 01:34:53 CST 2013"
                , "100001000008870,1357494968324,shy,tourist,Mon Jan 07 01:56:08 CST 2013"
                , "100001000008870,1357497202008,xc,tourist,Mon Jan 07 02:33:22 CST 2013"
                , "100001000008870,1357497751135,dc,tourist,Mon Jan 07 02:42:31 CST 2013"
                , "100001000008870,1357499124511,shy,tourist,Mon Jan 07 03:05:24 CST 2013"
                , "100001000008870,1357499510199,shy,tourist,Mon Jan 07 03:11:50 CST 2013"
                , "100001000008870,1357503917189,xc,tourist,Mon Jan 07 04:25:17 CST 2013"
                , "100001000008870,1357505618286,chp,tourist,Mon Jan 07 04:53:38 CST 2013"
                , "100001000008870,1357508340260,chy,tourist,Mon Jan 07 05:39:00 CST 2013"
                , "100001000008870,1357509526096,xc,tourist,Mon Jan 07 05:58:46 CST 2013"
                , "100001000008870,1357510929190,dc,tourist,Mon Jan 07 06:22:09 CST 2013"
                , "100001000008870,1357513105487,xc,tourist,Mon Jan 07 06:58:25 CST 2013"
                , "100001000008870,1357514740051,shy,tourist,Mon Jan 07 07:25:40 CST 2013"
                , "100001000008870,1357515379136,chy,tourist,Mon Jan 07 07:36:19 CST 2013"
                , "100001000008870,1357518718211,shy,tourist,Mon Jan 07 08:31:58 CST 2013"
                , "100001000008870,1357519187379,dc,tourist,Mon Jan 07 08:39:47 CST 2013"
                , "100001000008870,1357523081495,xc,tourist,Mon Jan 07 09:44:41 CST 2013"
                , "100001000008870,1357523842176,chp,tourist,Mon Jan 07 09:57:22 CST 2013"
                , "100001000008870,1357526061609,chp,tourist,Mon Jan 07 10:34:21 CST 2013"
                , "100001000008870,1357527186412,hd,tourist,Mon Jan 07 10:53:06 CST 2013"
                , "100001000008870,1357528076955,shy,tourist,Mon Jan 07 11:07:56 CST 2013"
                , "100001000008870,1357530593640,dc,tourist,Mon Jan 07 11:49:53 CST 2013"
                , "100001000008870,1357533395140,hd,tourist,Mon Jan 07 12:36:35 CST 2013"
                , "100001000008870,1357533674602,chy,tourist,Mon Jan 07 12:41:14 CST 2013"
                , "100001000008870,1357536429282,hd,tourist,Mon Jan 07 13:27:09 CST 2013"
                , "100001000008870,1357536843970,xc,tourist,Mon Jan 07 13:34:03 CST 2013"
                , "100001000008870,1357541572478,chy,tourist,Mon Jan 07 14:52:52 CST 2013"
                , "100001000008870,1357541642868,xc,tourist,Mon Jan 07 14:54:02 CST 2013"
                , "100001000008870,1357542102476,dc,tourist,Mon Jan 07 15:01:42 CST 2013"
                , "100001000008870,1357544092867,shy,tourist,Mon Jan 07 15:34:52 CST 2013"
                , "100001000008870,1357546991398,hd,tourist,Mon Jan 07 16:23:11 CST 2013"
                , "100001000008870,1357549167057,chy,tourist,Mon Jan 07 16:59:27 CST 2013"
                , "100001000008870,1357549476369,xc,tourist,Mon Jan 07 17:04:36 CST 2013"
                , "100001000008870,1357550768831,hd,tourist,Mon Jan 07 17:26:08 CST 2013"
                , "100001000008870,1357553950918,chy,tourist,Mon Jan 07 18:19:10 CST 2013"
                , "100001000008870,1357554980933,chp,tourist,Mon Jan 07 18:36:20 CST 2013"
                , "100001000008870,1357556758786,shy,tourist,Mon Jan 07 19:05:58 CST 2013"
                , "100001000008870,1357559375195,hd,tourist,Mon Jan 07 19:49:35 CST 2013"
                , "100001000008870,1357561818660,dc,tourist,Mon Jan 07 20:30:18 CST 2013"
                , "100001000008870,1357562000358,xc,tourist,Mon Jan 07 20:33:20 CST 2013"
                , "100001000008870,1357564446613,hd,tourist,Mon Jan 07 21:14:06 CST 2013"
                , "100001000008870,1357566858682,xc,tourist,Mon Jan 07 21:54:18 CST 2013"
                , "100001000008870,1357567348189,chy,tourist,Mon Jan 07 22:02:28 CST 2013"
                , "100001000008870,1357570017047,chp,tourist,Mon Jan 07 22:46:57 CST 2013"
                , "100001000008870,1357571517239,dc,tourist,Mon Jan 07 23:11:57 CST 2013"
                , "100001000008870,1357571806104,hd,tourist,Mon Jan 07 23:16:46 CST 2013"
                , "100001000008870,1357574450918,xc,tourist,Tue Jan 08 00:00:50 CST 2013"
                , "100001000008870,1357575135189,dc,tourist,Tue Jan 08 00:12:15 CST 2013"
                , "100001000008870,1357579202815,dc,tourist,Tue Jan 08 01:20:02 CST 2013"
                , "100001000008870,1357580441553,shy,tourist,Tue Jan 08 01:40:41 CST 2013"
                , "100001000008870,1357584732150,chp,tourist,Tue Jan 08 02:52:12 CST 2013"
                , "100001000008870,1357584892649,hd,tourist,Tue Jan 08 02:54:52 CST 2013"
                , "100001000008870,1357585867664,dc,tourist,Tue Jan 08 03:11:07 CST 2013"
                , "100001000008870,1357585904114,hd,tourist,Tue Jan 08 03:11:44 CST 2013"
                , "100001000008870,1357588996391,chy,tourist,Tue Jan 08 04:03:16 CST 2013"
                , "100001000008870,1357590416533,chy,tourist,Tue Jan 08 04:26:56 CST 2013"
                , "100001000008870,1357593254806,xc,tourist,Tue Jan 08 05:14:14 CST 2013"
                , "100001000008870,1357594511083,chp,tourist,Tue Jan 08 05:35:11 CST 2013"
                , "100001000008870,1357596693444,dc,tourist,Tue Jan 08 06:11:33 CST 2013"
                , "100001000008870,1357598427353,chp,tourist,Tue Jan 08 06:40:27 CST 2013"
                , "100001000008870,1357600789887,chp,tourist,Tue Jan 08 07:19:49 CST 2013"
                , "100001000008870,1357600955857,chy,tourist,Tue Jan 08 07:22:35 CST 2013"
                , "100001000008870,1357604164761,hd,tourist,Tue Jan 08 08:16:04 CST 2013"
                , "100001000008870,1357606451327,hd,tourist,Tue Jan 08 08:54:11 CST 2013"
                , "100001000008870,1357607565331,xc,tourist,Tue Jan 08 09:12:45 CST 2013"
                , "100001000008870,1357608098677,dc,tourist,Tue Jan 08 09:21:38 CST 2013"
                , "100001000008870,1357612672996,hd,tourist,Tue Jan 08 10:37:52 CST 2013"
                , "100001000008870,1357612764085,shy,tourist,Tue Jan 08 10:39:24 CST 2013"
                , "100001000008870,1357614084418,dc,tourist,Tue Jan 08 11:01:24 CST 2013"
                , "100001000008870,1357614379789,shy,tourist,Tue Jan 08 11:06:19 CST 2013"
                , "100001000008870,1357617757399,shy,tourist,Tue Jan 08 12:02:37 CST 2013"
                , "100001000008870,1357619336546,chy,tourist,Tue Jan 08 12:28:56 CST 2013"
                , "100001000008870,1357621738454,chp,tourist,Tue Jan 08 13:08:58 CST 2013"
                , "100001000008870,1357624069043,chp,tourist,Tue Jan 08 13:47:49 CST 2013"
                , "100001000008870,1357625145896,chy,tourist,Tue Jan 08 14:05:45 CST 2013"
                , "100001000008870,1357627109294,chp,tourist,Tue Jan 08 14:38:29 CST 2013"
                , "100001000008870,1357629979367,xc,tourist,Tue Jan 08 15:26:19 CST 2013"
                , "100001000008870,1357630044392,chp,tourist,Tue Jan 08 15:27:24 CST 2013"
                , "100001000008870,1357633435463,chp,tourist,Tue Jan 08 16:23:55 CST 2013"
                , "100001000008870,1357634329627,hd,tourist,Tue Jan 08 16:38:49 CST 2013"
                , "100001000008870,1357635834524,chy,tourist,Tue Jan 08 17:03:54 CST 2013"
                , "100001000008870,1357638836743,hd,tourist,Tue Jan 08 17:53:56 CST 2013"
                , "100001000008870,1357640032551,chp,tourist,Tue Jan 08 18:13:52 CST 2013"
                , "100001000008870,1357641023954,chy,tourist,Tue Jan 08 18:30:23 CST 2013"
                , "100001000008870,1357643694172,hd,tourist,Tue Jan 08 19:14:54 CST 2013"
                , "100001000008870,1357646293252,hd,tourist,Tue Jan 08 19:58:13 CST 2013"
                , "100001000008870,1357647298723,chp,tourist,Tue Jan 08 20:14:58 CST 2013"
                , "100001000008870,1357647382162,chp,tourist,Tue Jan 08 20:16:22 CST 2013"
                , "100001000008870,1357650403096,chp,tourist,Tue Jan 08 21:06:43 CST 2013"
                , "100001000008870,1357652813018,hd,tourist,Tue Jan 08 21:46:53 CST 2013"
                , "100001000008870,1357654481796,chp,tourist,Tue Jan 08 22:14:41 CST 2013"
                , "100001000008870,1357656196925,shy,tourist,Tue Jan 08 22:43:16 CST 2013"
                , "100001000008870,1357659115736,chy,tourist,Tue Jan 08 23:31:55 CST 2013"
                , "100001000008870,1357660479474,dc,tourist,Tue Jan 08 23:54:39 CST 2013"
                , "100001000008870,1357662184967,shy,tourist,Wed Jan 09 00:23:04 CST 2013"
                , "100001000008870,1357662923816,xc,tourist,Wed Jan 09 00:35:23 CST 2013"
                , "100001000008870,1357664877192,dc,tourist,Wed Jan 09 01:07:57 CST 2013"
                , "100001000008870,1357666634246,dc,tourist,Wed Jan 09 01:37:14 CST 2013"
                , "100001000008870,1357669147076,chp,tourist,Wed Jan 09 02:19:07 CST 2013"
                , "100001000008870,1357670544197,shy,tourist,Wed Jan 09 02:42:24 CST 2013"
                , "100001000008870,1357672398990,hd,tourist,Wed Jan 09 03:13:18 CST 2013"
                , "100001000008870,1357674143492,chp,tourist,Wed Jan 09 03:42:23 CST 2013"
                , "100001000008870,1357677264724,xc,tourist,Wed Jan 09 04:34:24 CST 2013"
                , "100001000008870,1357677378899,chp,tourist,Wed Jan 09 04:36:18 CST 2013"
                , "100001000008870,1357680455064,shy,tourist,Wed Jan 09 05:27:35 CST 2013"
                , "100001000008870,1357682182514,chy,tourist,Wed Jan 09 05:56:22 CST 2013"
                , "100001000008870,1357684401333,xc,tourist,Wed Jan 09 06:33:21 CST 2013"
                , "100001000008870,1357685087503,chp,tourist,Wed Jan 09 06:44:47 CST 2013"
                , "100001000008870,1357687550082,xc,tourist,Wed Jan 09 07:25:50 CST 2013"
                , "100001000008870,1357687823866,xc,tourist,Wed Jan 09 07:30:23 CST 2013"
                , "100001000008870,1357690290212,xc,tourist,Wed Jan 09 08:11:30 CST 2013"
                , "100001000008870,1357692468782,chp,tourist,Wed Jan 09 08:47:48 CST 2013"
                , "100001000008870,1357693391592,chp,tourist,Wed Jan 09 09:03:11 CST 2013"
                , "100001000008870,1357694729486,xc,tourist,Wed Jan 09 09:25:29 CST 2013"
                , "100001000008870,1357697924718,chp,tourist,Wed Jan 09 10:18:44 CST 2013"
                , "100001000008870,1357699892635,shy,tourist,Wed Jan 09 10:51:32 CST 2013"
                , "100001000008870,1357703429978,shy,tourist,Wed Jan 09 11:50:29 CST 2013"
                , "100001000008870,1357703975082,hd,tourist,Wed Jan 09 11:59:35 CST 2013"
                , "100001000008870,1357705746658,dc,tourist,Wed Jan 09 12:29:06 CST 2013"
                , "100001000008870,1357706630623,hd,tourist,Wed Jan 09 12:43:50 CST 2013"
                , "100001000008870,1357708858948,shy,tourist,Wed Jan 09 13:20:58 CST 2013"
                , "100001000008870,1357710809498,shy,tourist,Wed Jan 09 13:53:29 CST 2013"
                , "100001000008870,1357711324811,hd,tourist,Wed Jan 09 14:02:04 CST 2013"
                , "100001000008870,1357714298746,chp,tourist,Wed Jan 09 14:51:38 CST 2013"
                , "100001000008870,1357716169620,xc,tourist,Wed Jan 09 15:22:49 CST 2013"
                , "100001000008870,1357718315343,chy,tourist,Wed Jan 09 15:58:35 CST 2013"
                , "100001000008870,1357718988571,chp,tourist,Wed Jan 09 16:09:48 CST 2013"
                , "100001000008870,1357721671879,shy,tourist,Wed Jan 09 16:54:31 CST 2013"
                , "100001000008870,1357723294059,chp,tourist,Wed Jan 09 17:21:34 CST 2013"
                , "100001000008870,1357724101598,xc,tourist,Wed Jan 09 17:35:01 CST 2013"
                , "100001000008870,1357725658740,chy,tourist,Wed Jan 09 18:00:58 CST 2013"
                , "100001000008870,1357728416398,hd,tourist,Wed Jan 09 18:46:56 CST 2013"
                , "100001000008870,1357730878077,chy,tourist,Wed Jan 09 19:27:58 CST 2013"
                , "100001000008870,1357732241812,chp,tourist,Wed Jan 09 19:50:41 CST 2013"
                , "100001000008870,1357732958946,chp,tourist,Wed Jan 09 20:02:38 CST 2013"
                , "100001000008870,1357733637401,dc,tourist,Wed Jan 09 20:13:57 CST 2013"
                , "100001000008870,1357738595247,xc,tourist,Wed Jan 09 21:36:35 CST 2013"
                , "100001000008870,1357739092109,chy,tourist,Wed Jan 09 21:44:52 CST 2013"
                , "100001000008870,1357740661647,shy,tourist,Wed Jan 09 22:11:01 CST 2013"
                , "100001000008870,1357743560664,chp,tourist,Wed Jan 09 22:59:20 CST 2013"
                , "100001000008870,1357745380655,dc,tourist,Wed Jan 09 23:29:40 CST 2013"
                , "100001000008870,1357746253454,chp,tourist,Wed Jan 09 23:44:13 CST 2013"
                , "100001000008870,1357747851577,xc,tourist,Thu Jan 10 00:10:51 CST 2013"
                , "100001000008870,1357750271738,chp,tourist,Thu Jan 10 00:51:11 CST 2013"
                , "100001000008870,1357751536353,dc,tourist,Thu Jan 10 01:12:16 CST 2013"
                , "100001000008870,1357751718738,chy,tourist,Thu Jan 10 01:15:18 CST 2013"
                , "100001000008870,1357757434185,hd,tourist,Thu Jan 10 02:50:34 CST 2013"
                , "100001000008870,1357757772014,chy,tourist,Thu Jan 10 02:56:12 CST 2013"
                , "100001000008870,1357759492143,chy,tourist,Thu Jan 10 03:24:52 CST 2013"
                , "100001000008870,1357761360679,shy,tourist,Thu Jan 10 03:56:00 CST 2013"
                , "100001000008870,1357764353429,chy,tourist,Thu Jan 10 04:45:53 CST 2013"
                , "100001000008870,1357764459507,xc,tourist,Thu Jan 10 04:47:39 CST 2013"
                , "100001000008870,1357766367596,chp,tourist,Thu Jan 10 05:19:27 CST 2013"
                , "100001000008870,1357766723522,shy,tourist,Thu Jan 10 05:25:23 CST 2013"
                , "100001000008870,1357770015639,hd,tourist,Thu Jan 10 06:20:15 CST 2013"
                , "100001000008870,1357771555705,chp,tourist,Thu Jan 10 06:45:55 CST 2013"
                , "100001000008870,1357773883376,chp,tourist,Thu Jan 10 07:24:43 CST 2013"
                , "100001000008870,1357774072343,xc,tourist,Thu Jan 10 07:27:52 CST 2013"
                , "100001000008870,1357778118166,chy,tourist,Thu Jan 10 08:35:18 CST 2013"
                , "100001000008870,1357779569220,shy,tourist,Thu Jan 10 08:59:29 CST 2013"
                , "100001000008870,1357780307129,chy,tourist,Thu Jan 10 09:11:47 CST 2013"
                , "100001000008870,1357782011625,shy,tourist,Thu Jan 10 09:40:11 CST 2013"
                , "100001000008870,1357784755582,chy,tourist,Thu Jan 10 10:25:55 CST 2013"
                , "100001000008870,1357785467055,chy,tourist,Thu Jan 10 10:37:47 CST 2013"
                , "100001000008870,1357788535145,hd,tourist,Thu Jan 10 11:28:55 CST 2013"
                , "100001000008870,1357790214473,chp,tourist,Thu Jan 10 11:56:54 CST 2013"
                , "100001000008870,1357790994898,dc,tourist,Thu Jan 10 12:09:54 CST 2013"
                , "100001000008870,1357791516045,hd,tourist,Thu Jan 10 12:18:36 CST 2013"
                , "100001000008870,1357796099488,chy,tourist,Thu Jan 10 13:34:59 CST 2013"
                , "100001000008870,1357796352227,hd,tourist,Thu Jan 10 13:39:12 CST 2013"
                , "100001000008870,1357800395151,hd,tourist,Thu Jan 10 14:46:35 CST 2013"
                , "100001000008870,1357800495318,chp,tourist,Thu Jan 10 14:48:15 CST 2013"
                , "100001000008870,1357802663264,dc,tourist,Thu Jan 10 15:24:23 CST 2013"
                , "100001000008870,1357804366235,shy,tourist,Thu Jan 10 15:52:46 CST 2013"
                , "100001000008870,1357805062491,shy,tourist,Thu Jan 10 16:04:22 CST 2013"
                , "100001000008870,1357808330193,chp,tourist,Thu Jan 10 16:58:50 CST 2013"
                , "100001000008870,1357809387779,chp,tourist,Thu Jan 10 17:16:27 CST 2013"
                , "100001000008870,1357811588928,dc,tourist,Thu Jan 10 17:53:08 CST 2013"
                , "100001000008870,1357812848581,dc,tourist,Thu Jan 10 18:14:08 CST 2013"
                , "100001000008870,1357815374565,chp,tourist,Thu Jan 10 18:56:14 CST 2013"
                , "100001000008870,1357817693694,dc,tourist,Thu Jan 10 19:34:53 CST 2013"
                , "100001000008870,1357818544577,hd,tourist,Thu Jan 10 19:49:04 CST 2013"
                , "100001000008870,1357819664274,chy,tourist,Thu Jan 10 20:07:44 CST 2013"
                , "100001000008870,1357822661281,hd,tourist,Thu Jan 10 20:57:41 CST 2013"
                , "100001000008870,1357823969727,chy,tourist,Thu Jan 10 21:19:29 CST 2013"
                , "100001000008870,1357825172363,xc,tourist,Thu Jan 10 21:39:32 CST 2013"
                , "100001000008870,1357827189808,shy,tourist,Thu Jan 10 22:13:09 CST 2013"
                , "100001000008870,1357828852315,hd,tourist,Thu Jan 10 22:40:52 CST 2013"
                , "100001000008870,1357832052518,chp,tourist,Thu Jan 10 23:34:12 CST 2013"
                , "100001000008870,1357832152435,hd,tourist,Thu Jan 10 23:35:52 CST 2013"
        };
        for (int i = 0; i < signals.length; i++) {
            String signal = signals[i];
            signal = signal.substring(0, signal.lastIndexOf(","));
//            System.out.println(signal);
            sender.send(signal);
        }
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

        public void send(String signal) throws ParseException {
            session.write(signal);
        }

        public void close() {
            connector.dispose();
        }
    }

    private static long getTime(String s) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").parse(s + " +0000").getTime();
    }

    private static String getTime(long s) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(s - TimeZone.getDefault().getRawOffset()));
    }

    public static void main(String[] args) throws ParseException {
        System.out.println(getTime(1357001015454L));
        System.out.println(getTime(1357001569894L));
    }
}
