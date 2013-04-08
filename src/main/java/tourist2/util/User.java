package tourist2.util;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static tourist2.util.TimeUtil.ONE_HOUR;
import static tourist2.util.TimeUtil.getDays;

/**
 * 一个用户，有两个账户，分别为8~18点的账户，18~8点的账户
 * 每个账户都有自己状态(Worker,Tourist,Normal),每当账户的状态发生变更时，会通知用户。
 * 用户合并两个账户的状态，如果发现合并后的状态变更了，则发出通知给用户组的Listener。
 */
public class User implements UserGroup.Listener {
  private static Logger logger = LoggerFactory.getLogger(User.class);
  private final String imsi;
  private final UserGroup.Listener listener;
  private Accout accout8;
  private Accout accout18;

  public User(String imsi, UserGroup.Listener listener) throws IOException {
    this.imsi = imsi;
    this.listener = listener;
    accout8 = new Accout(8 * ONE_HOUR, imsi, this);
    accout18 = new Accout(18 * ONE_HOUR, imsi, this);
    logger.info(imsi + "~8~" + accout8.getEditLog().getFile());
    logger.info(imsi + "~18~" + accout18.getEditLog().getFile());
    System.out.println(imsi + "~8~" + accout8.getEditLog().getFile());
    System.out.println(imsi + "~18~" + accout18.getEditLog().getFile());
  }

  public void onSignal(long time, String loc, String cell) {
    try {
      long timeInDay = time - getDays(time);
      if (timeInDay >= 8 * ONE_HOUR && timeInDay < 18 * ONE_HOUR) {
          accout8.onSignal(time, loc, cell);
      } else {
          accout18.onSignal(time, loc, cell);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onAddTourist(long userTime, String imsi) {
    if (!accout8.isWorker() && !accout18.isWorker()) {
//      listener.onAddTourist(userTime, imsi);
      System.out.println(String.format("+t:%s %s", imsi, userTime));
//      if (logger.isInfoEnabled()){
//          logger.info(String.format("AddTourist: %s %s", imsi, userTime));
//      }
    }
  }

  @Override
  public void onAddWorker(long userTime, String imsi) {
    if (accout8.isWorker() || accout18.isWorker()) {
//      listener.onAddWorker(userTime, imsi);
    }
//    listener.onAddWorker(userTime, imsi);
    System.out.println(String.format("-w:%s %s", imsi, userTime));
//    if (logger.isInfoEnabled()){
//        logger.info(String.format("AddWorker: %s %s", imsi, userTime));
//    }
  }

  @Override
  public void onAddNormal(long userTime, String imsi) {
//    if (accout8.isWorker() || accout18.isWorker()) {
//      listener.onAddWorker(userTime, imsi);
//    }
    System.out.println(String.format("-n:%s %s", imsi, userTime));
  }

  public void updateGlobleTime(Long globalTime) {
    accout8.updateGlobleTime(globalTime);
    accout18.updateGlobleTime(globalTime);
  }
}
