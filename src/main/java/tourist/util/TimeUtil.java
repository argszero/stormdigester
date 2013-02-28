package tourist.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-2-28
 * Time: 下午12:48
 * To change this template use File | Settings | File Templates.
 */
public class TimeUtil {
    /**
     * 打印出零时区的时间： 0 -> 1970-01-01 00:00:00
     *
     * @param s
     * @return
     */
    public static String getTime(long s) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(s - TimeZone.getDefault().getRawOffset()));
    }

    public static void main(String[] args) {
        System.out.println(getTime(0));
    }
}
