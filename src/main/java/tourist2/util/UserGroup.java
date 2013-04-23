package tourist2.util;

import org.apache.commons.collections.Transformer;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 一组用户，其中，每个用户为一个User对象，当用户状态发生变更时，会发出通知给listener
 * 用户状态有三种：Worker，Tourist，Normal
 */
public class UserGroup implements Serializable {


    private Listener listener;
    private final Map<String, User> detectors = new HashMap<String, User>();
    private EditLog<AccountSnapshot> editLog8 = null;
    private EditLog<AccountSnapshot> editLog18 = null;

    public void init() {
        this.editLog8 = new EditLog<AccountSnapshot>(new File(System.getProperty("java.io.tmpdir"), "UserGroup8@" + this.hashCode() + new Random().nextInt(1000)), AccountSnapshot.class);
        this.editLog18 = new EditLog<AccountSnapshot>(new File(System.getProperty("java.io.tmpdir"), "UserGroup18@" + this.hashCode() + new Random().nextInt(1000)), AccountSnapshot.class);
    }

//  private final LazyMap detectors = (LazyMap) LazyMap.decorate(new HashMap(), new UserTransformer());

    private class UserTransformer implements Transformer, Serializable {
        @Override
        public Object transform(final Object input) {
            try {
                return new User((String) input, listener, editLog8,editLog18);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public UserGroup(Listener listener) {
        this.listener = listener;
    }

    public void onSignal(long time, String imsi, String loc, String cell) throws IOException {
        User user = detectors.get(imsi);
        if (user == null) {
            synchronized (detectors) {
                user = detectors.get(imsi);
                if (user == null) {
                    user = new User(imsi, listener, editLog8, editLog18);
                    detectors.put(imsi, user);
                    System.out.println("imsi:" + imsi + " editLog8:" + editLog8.getLogDirPath()
                            + " editLog18:" + editLog18.getLogDirPath());
                }
            }
        }
        user.onSignal(time, loc, cell);
    }

    public void updateGlobleTime(Long globalTime) {
        for (Object o : detectors.values()) {
            if (o instanceof User) {
                ((User) o).updateGlobleTime(globalTime);
            }
        }
    }

    public void close() {
        this.editLog8.close();
        this.editLog18.close();
    }

    public static interface Listener {
        void onAddTourist(long userTime, String imsi, Accout.Status preStatus);

        void onAddWorker(long userTime, String imsi, Accout.Status preStatus);

        void onAddNormal(long userTime, String imsi, Accout.Status preStatus);
    }

}
