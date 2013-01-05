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
import stormdigester.OrderedTimeWindow;
import stormdigester.StayTimeDetector;

import java.util.*;

/**

 */
public class WorkerDetectorBolt extends BaseRichBolt {
    private static final long ONE_DAY = 24 * 60 * 60 * 1000;
    private long now;
    private OutputCollector outputCollector;
    private final Set<String> tourits = new HashSet<String>();
    private final Map<Metrics, Status> statusMap;

    public WorkerDetectorBolt(Metrics... metricses) {
        statusMap = new HashMap<Metrics, Status>();
        for (Metrics metrics : metricses) {
            Status status = new Status(this, metrics);
            statusMap.put(metrics, status);
        }
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
            if (KbUtils.getInstance().isInside(loc, cell)) {//进入景区
                in(imsi, time);
                updateTourist(imsi, true);
            } else {//离开景区
                out(imsi, time);
                updateTourist(imsi, false);
            }
        } else if (tuple.getSourceStreamId().equals("updateTime")) {
            long time = tuple.getLong(0);
            updateTime(time);
        }
    }

    private void updateTourist(String imsi, boolean isInside) {
//        * 1. 当用户进入景区，发现他不是工作人员时，添加游客
//                * 2. 当用户不是工作人员，变为工作人员时，如果是游客，则删除游客
//                * 3. 当用户由工作人员，变为不是工作人员，判断用户当前是否在景区，在景区则添加游客
//                * 4. 当用户离开景区时，删除游客

        if (isInside) {
            boolean isWorker = isWorker(imsi);
            if (!isWorker) {
                if (!tourits.contains(imsi)) {
                    tourits.add(imsi);
                    this.outputCollector.emit("delta", new Values(1));
                }
            } else {
                if (tourits.contains(imsi)) {
                    tourits.remove(imsi);
                    this.outputCollector.emit("delta", new Values(-1));
                }
            }
        } else {
            if (tourits.contains(imsi)) {
                tourits.remove(imsi);
                this.outputCollector.emit("delta", new Values(-1));
            }
        }

    }

    private boolean isWorker(String imsi) {
        for (Status status : statusMap.values()) {
            if (status.workers.contains(imsi)) {
                return true;
            }
        }
        return false;
    }

    private void out(String imsi, long time) {
        for (Status status : statusMap.values()) {
            DaysStayTimeDetector detector = (DaysStayTimeDetector) status.detectors.get(imsi);
            detector.out(time);
        }
    }

    private void in(String imsi, long time) {
        for (Status status : statusMap.values()) {
            DaysStayTimeDetector detector = (DaysStayTimeDetector) status.detectors.get(imsi);
            detector.in(time);
        }
    }

    private void updateTime(long time) {
        if (time > now) {
            for (Status status : statusMap.values()) {
                status.updateTime(time);
            }
        }
        now = time;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("delta"));
    }

    public void onChange(String imsi, long startTime, long stayTime) {
        for (Status status : statusMap.values()) {
            status.onChange(imsi, startTime, stayTime);

        }
    }


    public static class Metrics {
        private final long startOfDay;
        private final long endOfDay;
        private final long stayTimeThreshold;

        public Metrics(long startOfDay, long endOfDay, long stayTimeThreshold) {
            this.startOfDay = startOfDay;
            this.endOfDay = endOfDay;
            this.stayTimeThreshold = stayTimeThreshold;
        }
    }

    private static class Status {
        private WorkerDetectorBolt workerDetectorBolt;
        private Metrics metrics;
        private final LazyMap detectors = (LazyMap) LazyMap.decorate(new HashMap(), new Transformer() {
            @Override
            public Object transform(final Object input) {
                return new DaysStayTimeDetector(metrics.startOfDay, metrics.endOfDay, metrics.stayTimeThreshold, new DaysStayTimeDetector.Listener() {
                    @Override
                    public void onChange(long startTime, long stayTime) {
                        workerDetectorBolt.onChange((String) input, stayTime, startTime);
                    }
                });
            }
        });

        private final LazyMap workerDays = (LazyMap) LazyMap.decorate(new HashMap(), new Transformer() {
            @Override
            public Object transform(final Object input) {
                return new HashSet<Long>();
            }
        });

        public Status(WorkerDetectorBolt workerDetectorBolt, Metrics metrics) {
            this.workerDetectorBolt = workerDetectorBolt;
            this.metrics = metrics;
        }

        private long lastStartTime = 0;
        private final Set<String> workers = new HashSet<String>();

        public void onChange(String imsi, long startTime, long stayTime) {
            HashSet<Long> workerDays = (HashSet<Long>) this.workerDays.get(imsi);
            if (stayTime > metrics.stayTimeThreshold) {
                workerDays.add(startTime);
            } else {
                workerDays.remove(startTime);
            }
            updateWorkerList(imsi);
            lastStartTime = workerDetectorBolt.now;
        }

        private void updateWorkerList(String imsi) {
            HashSet<Long> workerDays = (HashSet<Long>) this.workerDays.get(imsi);
            if (workerDays.size() > 5) {
                for (Iterator<Long> iterator = workerDays.iterator(); iterator.hasNext(); ) {
                    Long next = iterator.next();
                    if (workerDetectorBolt.now - next > 10 * ONE_DAY) {
                        iterator.remove();
                    }
                }
                if (workerDays.size() == 0) {
                    this.workerDays.remove(imsi);
                    updateWorkers(imsi, false);
                } else if (workerDays.size() > 5) {
                    updateWorkers(imsi, true);
                }
            } else {
                updateWorkers(imsi, false);
            }
        }

        private void updateWorkers(String imsi, boolean isWorker) {
            if (isWorker) {
                if (!workers.contains(imsi)) {
                    workers.add(imsi);
                    workerDetectorBolt.updateTourist(imsi, isInside(imsi));
                }
            } else {
                if (workers.contains(imsi)) {
                    workers.remove(imsi);
                    workerDetectorBolt.updateTourist(imsi, isInside(imsi));
                }
            }
        }

        private boolean isInside(String imsi) {
            DaysStayTimeDetector detector = (DaysStayTimeDetector) detectors.get(imsi);
            OrderedTimeWindow.Event<StayTimeDetector.Status> lastEvent = detector.getLastEvent();
            return (lastEvent != null && lastEvent.data == StayTimeDetector.Status.IN);
        }

        public void updateTime(long time) {
            for (Object detector : detectors.values()) {
                ((DaysStayTimeDetector) detector).update(time);
            }
            if (lastStartTime < time) {
                for (String imsi : workers) {
                    updateWorkerList(imsi);
                }
                lastStartTime = time;
            }
        }
    }
}
