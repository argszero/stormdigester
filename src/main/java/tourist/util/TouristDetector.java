package tourist.util;

import java.util.Set;

/**
 *
 */
public class TouristDetector implements MetricsDetector.Listener {
    private Set<String> tourists;
    private Set<String> workers;
    private MetricsDetector[] detectors;
    private long currentTime;
    private Listener listener;

    public TouristDetector(Listener listener, MetricsDetector.Metrics... metricses) {
        detectors = new MetricsDetector[metricses.length];
        for (int i = 0; i < metricses.length; i++) {
            detectors[i] = new MetricsDetector(this,metricses[i]);
        }
        this.listener = listener;
    }

    public void onSignaling(String imsi, long time, String loc, String cell) {
        boolean isInside = KbUtils.getInstance().isInside(loc, cell);
        for (MetricsDetector detector : detectors) {
            if (isInside) {
                detector.in(imsi, time);
            } else {
                detector.out(imsi, time);
            }
        }
        if (workers.contains(imsi)) {
            if (tourists.contains(imsi)) {
                tourists.remove(imsi);
                listener.removeTourist(imsi);
            }
        } else {
            if (isInside) {
                if (!workers.contains(imsi)) {
                    if (!tourists.contains(imsi)) {
                        tourists.add(imsi);
                        listener.addTourist(imsi);
                    }
                }
            } else {
                if (!workers.contains(imsi)) {
                    if (tourists.contains(imsi)) {
                        tourists.remove(imsi);
                        listener.removeTourist(imsi);
                    }
                }
            }
        }
    }

    @Override
    public void onChange(String imsi, int days, int daysThreshold) {
        if (days >= daysThreshold) {
            if (!workers.contains(imsi)) {
                workers.add(imsi);
                if (tourists.contains(imsi)) {
                    tourists.remove(imsi);
                    listener.removeTourist(imsi);
                }
            }
        } else {//在一个 detector 里不是worker，可能在另一个 detector 里是worker
            for (MetricsDetector detector : detectors) {
                if (detector.isWorker(currentTime, imsi)) {
                    return;
                }
            }
            if (workers.contains(imsi)) {
                workers.remove(imsi);
            }
        }
    }

    public void updateTime(long time) {
        if (currentTime < time) {
            for (MetricsDetector detector : detectors) {
                detector.updateTime(time);
            }
            currentTime = time;
        }
    }

    public static interface Listener {
        void addTourist(String imsi);

        void removeTourist(String imsi);
    }
}
