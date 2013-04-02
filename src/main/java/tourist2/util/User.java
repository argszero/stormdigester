package tourist2.util;

import java.io.IOException;

import static tourist2.util.TimeUtil.ONE_HOUR;

/**
 * 一个用户，有两个账户，分别为8~18点的账户，18~8点的账户
 * 每个账户都有自己状态(Worker,Tourist,Normal),每当账户的状态发生变更时，会通知用户。
 * 用户合并两个账户的状态，如果发现合并后的状态变更了，则发出通知给用户组的Listener。
 */
public class User implements UserGroup.Listener {
  private final String imsi;
  private final UserGroup.Listener listener;
  private Accout accout8;
  private Accout accout18;

  public User(String imsi, UserGroup.Listener listener) throws IOException {
    this.imsi = imsi;
    this.listener = listener;
    accout8 = new Accout(8 * ONE_HOUR, imsi, this);
    accout18 = new Accout(18 * ONE_HOUR, imsi, this);
  }

  public void onSignal(long time, String loc, String cell) {
    try {
      accout8.onSignal(time, loc, cell);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onAddTourist(long userTime, String imsi) {
    if (!accout8.isWorker() && !accout18.isWorker()) {
      listener.onAddTourist(userTime, imsi);
    }
  }

  @Override
  public void onAddWorker(long userTime, String imsi) {
    listener.onAddWorker(userTime, imsi);
  }

  @Override
  public void onAddNormal(long userTime, String imsi) {
    if (accout8.isWorker() || accout18.isWorker()) {
      listener.onAddWorker(userTime, imsi);
    }
  }

  public void updateGlobleTime(Long globalTime) {
    accout8.updateGlobleTime(globalTime);
    accout18.updateGlobleTime(globalTime);
  }
}
