package tourist2.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static tourist2.util.TimeUtil.*;

/**
 * 账户，代表一个用户在某个统计方式下的状态，比如（如果按照8~18点统计，他应该是Tourist?Worker?Normal?
 * 每个账户会保存三个状态：
 * 1. 最后一次信令时间：lastTime
 * 2. 最后一次信令时的状态：lastInside
 * 3. 最近10天的停留时间：recentDays
 * <p/>
 * 每次账户收到一个新的信令，都会记录到EditLog里。当发现新收到的信令比之前的信令早时，从EditLog读取之前的一个同步点，
 * 从这个同步点开始，后续的记录重新排序后重新计算。
 */
public class Accout {
  enum Status {
    Worker, Normal, Tourist
  }

  private final long start;
  private String imsi;
  private final UserGroup.Listener listener;
  private long lastStart;
  private long lastTime = 0; // 最后一次信令时间
  private boolean lastInside = false; // 最后一次信令时的状态
  private long[] recentDays = new long[10]; // 最近10天的停留时间
  private Status status = Status.Normal;
  private final EditLog editLog;

  public Accout(long start, String imsi, UserGroup.Listener listener) throws IOException {
    this.start = start;
    this.imsi = imsi;
    this.listener = listener;
    this.lastStart = start;
    this.editLog = new EditLog(imsi);
  }

  public void onSignal(long time, String loc, String cell) throws IOException {
    lastStart = getDays(time) + start;
    System.out.println(String.format("[lastStart]%s~%s", imsi, getTime(lastStart)));
    boolean isInside = KbUtils.getInstance().isInside(loc, cell);
    this.editLog.append(time, isInside, lastTime, lastInside, recentDays);
    if (time >= lastTime) {//正序
      order(time, isInside);
    } else {//乱序,很少发生，不需要考虑效率
      List<Object[]> olds = new ArrayList<Object[]>();
      editLog.readFromTail();
      long atime = 0;
      boolean aLogStatus = false;
      do {
        atime = editLog.getTime();
        boolean aInside = editLog.getInside();
        olds.add(new Object[]{atime, aInside});
        aLogStatus = editLog.getLogStatus();
      } while (atime > (time + 20 * ONE_MINUTE) && editLog.next() && !aLogStatus);
      //修改为之前的状态
      this.lastTime = editLog.getLastTime();
      this.lastInside = editLog.getLastInside();
      this.recentDays = editLog.getRecentDays();

      olds.add(new Object[]{time, loc, cell});
      Collections.sort(olds, new Comparator<Object[]>() { //由小到大排序
        @Override
        public int compare(Object[] o1, Object[] o2) {
          return (int) ((Long) o1[0] - (Long) o2[0]);
        }
      });
      for (Object[] old : olds) {
        order((Long) old[0], KbUtils.getInstance().isInside(loc, cell));
      }
    }
    check(time);
  }

  private void order(long time, boolean inside) {
    do {
      if (lastInside) { // 上次在景区则添加本次停留时间
        if (start == 8 * ONE_HOUR) {
            recentDays[9] += (Math.min(time, lastStart + 10 * ONE_HOUR) - lastTime);
        } else if (start == 18 * ONE_HOUR) {
            recentDays[9] += (Math.min(time, lastStart + 14 * ONE_HOUR) - lastTime);
        }

      }
      if (time < lastStart + ONE_DAY) {
        lastTime = time;
      } else {
        lastTime = lastStart + ONE_DAY;
        for (int i = 0; i < recentDays.length - 1; i++) {
          recentDays[i] = recentDays[i + 1];
        }
        recentDays[9] = 0;
        lastStart += ONE_DAY;
      }
      lastInside = inside;
    } while (time > lastStart + ONE_DAY);
  }

  public boolean isWorker() {
    int i = 0;
    if (start == 8 * ONE_HOUR) {
        for (Object o : recentDays) {
            if (o instanceof Long) {
                if (Long.class.cast(o) > 3 * ONE_HOUR) {
                    if (++i > 5) {
                        return true;
                    }
                }
            }
        }
    } else if (start == 18 * ONE_HOUR) {
        for (Object o : recentDays) {
            if (o instanceof Long) {
                if (Long.class.cast(o) > 5 * ONE_HOUR) {
                    if (++i > 5) {
                        return true;
                    }
                }
            }
        }
    }
    return false;
  }

  public void updateGlobleTime(Long globalTime) {
    if (globalTime > lastTime) {
      order(globalTime, lastInside);
    }
    check(globalTime);
  }

  private void check(long time) {
    int i = 0;
    if (start == 8 * ONE_HOUR) {
        for (Object o : recentDays) {
            if (o instanceof Long) {
                if (Long.class.cast(o) > 3 * ONE_HOUR) {
                    if (++i > 5) {
                        if (status != Status.Worker) {
                            status = Status.Worker;
                            this.listener.onAddWorker(time, imsi);
                        }
                    }
                }
            }
        }
    } else if (start == 18 * ONE_HOUR) {
        for (Object o : recentDays) {
            if (o instanceof Long) {
                if (Long.class.cast(o) > 5 * ONE_HOUR) {
                    if (++i > 5) {
                        if (status != Status.Worker) {
                            status = Status.Worker;
                            this.listener.onAddWorker(time, imsi);
                        }
                    }
                }
            }
        }
    }

    if (lastInside) {
      if (status != Status.Worker) {
        this.listener.onAddTourist(time, imsi);
      } else {
        this.listener.onAddWorker(time, imsi);
      }
    } else {
      if (status != Status.Worker) {
        this.listener.onAddNormal(time, imsi);
      } else {
        this.listener.onAddWorker(time, imsi);
      }
    }
  }

    public EditLog getEditLog() {
        return editLog;
    }
}
