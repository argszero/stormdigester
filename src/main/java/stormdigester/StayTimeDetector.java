package stormdigester;

import static java.lang.Math.max;

/**
 * 针对特定用户，检测停留时间。
 * 当停留时间变化时时，回调处理函数。
 * <p/>
 * 窗口保存最近10分钟的数据，当窗口数据变化时，重新计算停留时间
 */
public class StayTimeDetector implements OrderedTimeWindow.Listener<StayTimeDetector.Status> {
    private static final long ONE_MINUTE = 60 * 1000;
    private long stayTime;
    private OrderedTimeWindow orderedTimeWindow = new OrderedTimeWindow(this, 13 * ONE_MINUTE, 2 * ONE_MINUTE);
    private long startTime;
    private Listener listener;

    public StayTimeDetector(Listener listener) {
        this.listener = listener;
    }

    /**
     * 当新的统计周期开始时调用这个方法。
     *
     * @param startTime
     */
    public void reset(long startTime) {
        this.startTime = startTime;
        this.stayTime = 0;
    }

    public void in(long time) {
        orderedTimeWindow.add(time, Status.IN);
    }

    public void out(long time) {
        orderedTimeWindow.add(time, Status.OUT);
    }

    public void update(long time) {
        orderedTimeWindow.update(time);
    }
//
//    @Override
//    public void onAppend(OrderedTimeWindow.Event<Status> pre, OrderedTimeWindow.Event<Status> current) {
//        append(pre, current);
//    }

    @Override
    public void onInsert(OrderedTimeWindow.Event<Status> pre, OrderedTimeWindow.Event<Status> currrent, OrderedTimeWindow.Event<Status>[] nexts) {
        OrderedTimeWindow.Event<Status>[] oldEvents = new OrderedTimeWindow.Event[nexts.length + 1];
        oldEvents[0] = pre;
        System.arraycopy(nexts, 0, oldEvents, 1, nexts.length);
        for (int i = oldEvents.length - 1; i > 0; i--) {
            rollback(oldEvents[i - 1], oldEvents[i]);
        }
        OrderedTimeWindow.Event<Status>[] newEvents = new OrderedTimeWindow.Event[nexts.length + 2];
        newEvents[0] = pre;
        newEvents[1] = currrent;
        System.arraycopy(nexts, 0, newEvents, 2, nexts.length);
        for (int i = newEvents.length - 1; i > 0; i--) {
            append(newEvents[i - 1], newEvents[i]);
        }
    }

    private void append(OrderedTimeWindow.Event<Status> pre, OrderedTimeWindow.Event<Status> current) {
        if (pre != null && pre.data == Status.IN) { //如果上次在里面，则累加停留时间。其他情况下停留时间不变。
            updateStayTime(current.time - max(startTime, pre.time));
        }
    }

    private void rollback(OrderedTimeWindow.Event<Status> pre, OrderedTimeWindow.Event<Status> current) {
        if (pre.data == Status.IN) { //如果上次在里面，则回退停留时间。其他情况下停留时间不变。
            updateStayTime(-(max(current.time, stayTime) - max(stayTime, pre.time)));
        }
    }


    private void updateStayTime(long delta) {
        stayTime += delta;
        this.listener.onChange(stayTime);
    }

    public static interface Listener {
        void onChange(long stayTime);
    }

    public static enum Status {
        IN, OUT
    }
}
