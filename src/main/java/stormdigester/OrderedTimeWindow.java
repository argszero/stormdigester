package stormdigester;


import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class OrderedTimeWindow<T> {
    private Listener listener;
    private long size;
    private long slotSize;
    private Circular<Slot<Event<T>>> slots;
    private Event<T> lastEvent = null; //除了保存最后10分钟以内没有Event发生，就需要最后一个Event来判断上次的状态

    public OrderedTimeWindow(Listener<T> listener, long size, long slotSize) {
        this.listener = listener;
        this.size = size;
        this.slotSize = slotSize;
        //保证当slot切换，即有一个slot里没有值时，剩余的slot里能存的下整个slotSize
        int slotCount = (size % slotSize == 0) ? ((int) (size / slotSize) + 1) : ((int) (size / slotSize) + 2);
        this.slots = new Circular<Slot<Event<T>>>(slotCount);
    }

    public void add(long time, T data) {
        Event<T> current = new Event<T>(time, data);
        Event<T> pre = null;
        List<Event<T>> nexts = new ArrayList<Event<T>>();
        for (int i = 0; i < slots.size(); i++) { //从最近的slot开始遍历，一般情况下是第一个slot
            Slot<Event<T>> slot = slots.get(i);
            if (slot != null && slot.startTime <= time && slot.endTime > time) {
                for (int j = slot.list.size(); j >= 0; j--) {  //从最后一个event开始遍历
                    pre = slot.list.get(j);
                    if (pre.time <= time) {
                        slot.list.add(j + 1, current);
                        lastEvent = current;
                        this.listener.onInsert(pre, current, nexts.toArray(new Event[nexts.size()]));
                        break;
                    } else {
                        nexts.add(pre);
                    }
                }
                break;
            } else if (slot != null && time >= slot.endTime) { //创建新的slot
                pre = lastEvent;
                while (time >= slot.endTime) {
                    Slot<Event<T>> newSlot = new Slot<Event<T>>(slot.endTime, slot.endTime + slotSize);
                    slots.add(newSlot);
                    slot = newSlot;
                }
                slot.list.add(current);
                lastEvent = current;
                this.listener.onInsert(pre, current, new Event[0]);
                break;
            } else if (slot == null) {
                for (int j = slots.size() - 1; j >= 0; j--) {
                    slot = new Slot<Event<T>>(time - slotSize * j, time - slotSize * j + slotSize);
                    slots.add(slot);
                }
                slot.list.add(current);
                lastEvent = current;
                this.listener.onInsert(pre, current, new Event[0]);
                break;
            }
        }

    }

    public void update(long time) {
        //可以清空endTime< time - size 之前的slot,以提高内存使用率。也可以什么都不做
        //因为大部分情况下都不会引起slot切换，小部分的也只是切换一个slot，所以我们这里只需要看看最后一个slot
        Slot<Event<T>> latest = slots.get(slots.size());
        if (latest != null && latest.endTime < time - size) {
            Slot<Event<T>> newest = slots.get(0);
            Slot<Event<T>> slot = new Slot<Event<T>>(newest.endTime, newest.endTime + slotSize);
            slots.add(slot);
        }
    }

    private static class Slot<T> {
        private long startTime;
        private long endTime;
        private List<T> list = new ArrayList<T>();

        private Slot(long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }


    public static interface Listener<T> {
        void onInsert(Event<T> pre, Event<T> currrent, Event<T>[] nexts);

    }

    public static class Event<T> {
        public long time;
        public T data;

        public Event(long time, T data) {
            this.time = time;
            this.data = data;
        }
    }
}
