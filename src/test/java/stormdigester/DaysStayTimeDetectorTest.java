package stormdigester;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.omg.CORBA.LongHolder;
import tourist2.util.DaysStayTimeDetector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * DaysStayTimeDetector Tester.
 *
 * @author <Authors name>
 * @version 1.0
 */
public class DaysStayTimeDetectorTest {
    private static final long ONE_HOUR = 60 * 60 * 1000;
    private static final long ONE_MINUTE = 60 * 1000;

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * 测试8到18点之间，用户停留超过3个小时
     */
    @Test
    public void testIn1() throws Exception {
        final LongHolder stayTimeHolder = new LongHolder(0);
        DaysStayTimeDetector daysStayTimeDetector = new DaysStayTimeDetector("100001000035254", "08:00~18:00", 8 * ONE_HOUR, 18 * ONE_HOUR, 3 * ONE_HOUR, new DaysStayTimeDetector.Listener() {
            @Override
            public void onChange(long startTime, long stayTime) {
                stayTimeHolder.value = stayTime;
            }
        });
        daysStayTimeDetector.in(getTime("2013-01-04 08:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 09:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 10:00:00"));
        Assert.assertEquals(0, stayTimeHolder.value); //停留2个小时，一直小于3个小时，不更新
        daysStayTimeDetector.in(getTime("2013-01-04 11:00:00"));
        Assert.assertEquals(3 * ONE_HOUR, stayTimeHolder.value); //停留3个小时，原先小于3个小时，现在等于3个小时，更新
        daysStayTimeDetector.in(getTime("2013-01-04 12:00:00"));
        Assert.assertEquals(4 * ONE_HOUR, stayTimeHolder.value); //停留4个小时，原先等于3个小时，现在大于3个小时，更新
        daysStayTimeDetector.in(getTime("2013-01-04 13:00:00"));
        Assert.assertEquals(4 * ONE_HOUR, stayTimeHolder.value); //停留5个小时，原先大于3个小时，现在还大于3个小时，不更新
        daysStayTimeDetector.in(getTime("2013-01-04 14:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 15:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 16:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 17:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 18:00:00"));
        Assert.assertEquals(4 * ONE_HOUR, stayTimeHolder.value); //到达统计周期结束时间，但没开始下次统计周期，不更新
        daysStayTimeDetector.in(getTime("2013-01-04 19:00:00"));
        Assert.assertEquals(4 * ONE_HOUR, stayTimeHolder.value); //过了统计周期结束时间，但没开始下次统计周期，不更新
        daysStayTimeDetector.in(getTime("2013-01-04 20:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 21:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 22:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 23:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 00:00:00"));
        Assert.assertEquals(4 * ONE_HOUR, stayTimeHolder.value); //开始新的一天，但没开始下次统计周期，不更新
        daysStayTimeDetector.in(getTime("2013-01-05 01:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 02:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 03:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 04:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 05:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 06:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 07:00:00"));
        Assert.assertEquals(4 * ONE_HOUR, stayTimeHolder.value); //即将开始下次统计，但没开始下次统计周期，不更新
        daysStayTimeDetector.in(getTime("2013-01-05 08:00:00"));
        Assert.assertEquals(0 * ONE_HOUR, stayTimeHolder.value); //开始下次统计周期，更新
        daysStayTimeDetector.in(getTime("2013-01-05 09:00:00"));
        Assert.assertEquals(0 * ONE_HOUR, stayTimeHolder.value); //开始下次统计周期，原先小于3个小时，现在还是小于3个小时，不更新
        daysStayTimeDetector.in(getTime("2013-01-05 10:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 11:00:00"));
        Assert.assertEquals(3 * ONE_HOUR, stayTimeHolder.value); //开始下次统计周期，原先等于3个小时，现在还是小于3个小时，不更新
    }

    /**
     * 测试8到18点之间，用户连续两天满足，且中间没有不满足的时候
     */
    @Test
    public void testIn2() throws Exception {
        final LongHolder stayTimeHolder = new LongHolder(0);
        DaysStayTimeDetector daysStayTimeDetector = new DaysStayTimeDetector("100001000035254", "08:00~18:00", 8 * ONE_HOUR, 18 * ONE_HOUR, 3 * ONE_HOUR, new DaysStayTimeDetector.Listener() {
            @Override
            public void onChange(long startTime, long stayTime) {
                stayTimeHolder.value = stayTime;
            }
        });
        daysStayTimeDetector.in(getTime("2013-01-04 08:00:00"));
        daysStayTimeDetector.out(getTime("2013-01-04 11:01:00"));
        Assert.assertEquals(3 * ONE_HOUR + 1 * ONE_MINUTE, stayTimeHolder.value); //停留2个小时，一直小于3个小时，不更新
        daysStayTimeDetector.in(getTime("2013-01-05 08:00:00"));
        daysStayTimeDetector.out(getTime("2013-01-05 11:02:00"));
        Assert.assertEquals(3 * ONE_HOUR + 2 * ONE_MINUTE, stayTimeHolder.value); //停留2个小时，一直小于3个小时，不更新
    }

    /**
     * 测试18到8点之间，用户停留超过3个小时
     */
    @Test
    public void testIn3() throws Exception {
        final LongHolder stayTimeHolder = new LongHolder(0);
        DaysStayTimeDetector daysStayTimeDetector = new DaysStayTimeDetector("100001000035254", "18:00~08:00", 18 * ONE_HOUR, 8 * ONE_HOUR, 3 * ONE_HOUR, new DaysStayTimeDetector.Listener() {
            @Override
            public void onChange(long startTime, long stayTime) {
                stayTimeHolder.value = stayTime;
            }
        });
        daysStayTimeDetector.in(getTime("2013-01-04 08:00:00"));
        Assert.assertEquals(0 * ONE_HOUR, stayTimeHolder.value); //停留时间为0小时，不更新
        daysStayTimeDetector.in(getTime("2013-01-04 09:00:00"));
        Assert.assertEquals(0 * ONE_HOUR, stayTimeHolder.value); //停留时间为1小时,原先小于3小时，现在还小于3小时，不更新
        daysStayTimeDetector.in(getTime("2013-01-04 10:00:00"));
        Assert.assertEquals(0 * ONE_HOUR, stayTimeHolder.value); //停留时间为2小时,原先小于3小时，现在还小于3小时，不更新
        daysStayTimeDetector.in(getTime("2013-01-04 11:00:00"));
        Assert.assertEquals(3 * ONE_HOUR, stayTimeHolder.value);  //停留时间为3小时,原先小于3小时，现在等于3小时，更新
        daysStayTimeDetector.in(getTime("2013-01-04 12:00:00"));
        Assert.assertEquals(4 * ONE_HOUR, stayTimeHolder.value);  //停留时间为4小时,原先等于3小时，现在大于3小时，更新
        daysStayTimeDetector.in(getTime("2013-01-04 13:00:00"));
        Assert.assertEquals(4 * ONE_HOUR, stayTimeHolder.value);  //停留时间为5小时,原先大于3小时，现在还大于3小时，不更新
        daysStayTimeDetector.in(getTime("2013-01-04 14:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 15:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 16:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 17:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 18:00:00"));
        Assert.assertEquals(0 * ONE_HOUR, stayTimeHolder.value);     //开始新的一天，开始新的统计周期，更新
        daysStayTimeDetector.in(getTime("2013-01-04 19:00:00"));
        Assert.assertEquals(0 * ONE_HOUR, stayTimeHolder.value);     //停留1个小时，新的统计周期,停留时间一直小于3个小时，不更新
        daysStayTimeDetector.in(getTime("2013-01-04 20:00:00"));
        Assert.assertEquals(0 * ONE_HOUR, stayTimeHolder.value);     //停留2个小时，新的统计周期,停留时间一直小于3个小时，不更新
        daysStayTimeDetector.in(getTime("2013-01-04 21:00:00"));
        Assert.assertEquals(3 * ONE_HOUR, stayTimeHolder.value);     //停留3个小时，新的统计周期,原先小于3个小时，现在等于3个小时，更新
        daysStayTimeDetector.in(getTime("2013-01-04 22:00:00"));
        Assert.assertEquals(4 * ONE_HOUR, stayTimeHolder.value);     //停留4个小时，新的统计周期,原先等于3个小时，现在大于3个小时，更新
        daysStayTimeDetector.in(getTime("2013-01-04 23:00:00"));
        Assert.assertEquals(4 * ONE_HOUR, stayTimeHolder.value);     //停留5个小时，新的统计周期,原先大于3个小时，现在大于3个小时，不更新
        daysStayTimeDetector.in(getTime("2013-01-05 00:00:00"));
        Assert.assertEquals(4 * ONE_HOUR, stayTimeHolder.value);  //开始新的一天，但还是这个统计周期，不更新
        daysStayTimeDetector.in(getTime("2013-01-05 01:00:00"));
        Assert.assertEquals(4 * ONE_HOUR, stayTimeHolder.value);  //新的一天，但还是这个统计周期，不更新
        daysStayTimeDetector.in(getTime("2013-01-05 02:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 03:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 04:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 05:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 06:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 07:00:00"));
        Assert.assertEquals(4 * ONE_HOUR, stayTimeHolder.value);  //新的一天，但还是这个统计周期，不更新
        daysStayTimeDetector.in(getTime("2013-01-05 08:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 09:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 10:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-05 11:00:00"));
    }

    /**
     * 测试有进有出
     */
    @Test
    public void testIn4() throws Exception {
        final LongHolder stayTimeHolder = new LongHolder(0);
        DaysStayTimeDetector daysStayTimeDetector = new DaysStayTimeDetector("100001000035254", "18:00~08:00", 18 * ONE_HOUR, 8 * ONE_HOUR, 3 * ONE_HOUR, new DaysStayTimeDetector.Listener() {
            @Override
            public void onChange(long startTime, long stayTime) {
                stayTimeHolder.value = stayTime;
            }
        });
        daysStayTimeDetector.in(getTime("2013-01-04 08:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 09:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 10:00:00"));
        Assert.assertEquals(0 * ONE_HOUR, stayTimeHolder.value);
        daysStayTimeDetector.out(getTime("2013-01-04 10:58:00")); //出去了两分钟
        daysStayTimeDetector.in(getTime("2013-01-04 11:00:00"));
        Assert.assertEquals(0 * ONE_HOUR, stayTimeHolder.value); //还差两分钟，所以不应该更新
        daysStayTimeDetector.in(getTime("2013-01-04 11:01:00"));
        Assert.assertEquals(0 * ONE_HOUR, stayTimeHolder.value); //还差一分钟，所以不应该更新
        daysStayTimeDetector.in(getTime("2013-01-04 11:02:00"));
        Assert.assertEquals(3 * ONE_HOUR, stayTimeHolder.value);  //停留时间为3个小时，更新
    }

    /**
     * 测试乱序
     */
    @Test
    public void testIn5() throws Exception {
        final LongHolder stayTimeHolder = new LongHolder(0);
        DaysStayTimeDetector daysStayTimeDetector = new DaysStayTimeDetector("100001000035254", "18:00~08:00", 18 * ONE_HOUR, 8 * ONE_HOUR, 3 * ONE_HOUR, new DaysStayTimeDetector.Listener() {
            @Override
            public void onChange(long startTime, long stayTime) {
                stayTimeHolder.value = stayTime;
            }
        });
        daysStayTimeDetector.in(getTime("2013-01-04 08:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 09:00:00"));
        daysStayTimeDetector.in(getTime("2013-01-04 10:00:00"));
        Assert.assertEquals(0 * ONE_HOUR, stayTimeHolder.value);
        daysStayTimeDetector.in(getTime("2013-01-04 11:00:00"));
        Assert.assertEquals(3 * ONE_HOUR, stayTimeHolder.value); //停留时间为三个小时
        daysStayTimeDetector.out(getTime("2013-01-04 10:58:00"));
        //乱序到达，两分钟之前就出去了，停留时间应该还差2分钟
        //由于会先回退，即停留时间变为两个小时，然后再计算新增的，即停留时间变为两个小时58分钟
        //其中，第一次变化由等于3个小时变化为小于3个小时，更新。第二次变化还是小于3个小时，不更新
        //因此，这里获取的时间应该为两个小时
        Assert.assertEquals(3 * ONE_HOUR - 2 * ONE_MINUTE, daysStayTimeDetector.getStayTime());
        Assert.assertEquals(2 * ONE_HOUR, stayTimeHolder.value);

        daysStayTimeDetector.in(getTime("2013-01-04 11:01:00"));
        Assert.assertEquals(3 * ONE_HOUR - 1 * ONE_MINUTE, daysStayTimeDetector.getStayTime());
        Assert.assertEquals(2 * ONE_HOUR, stayTimeHolder.value); //还差1分钟，不更新

        daysStayTimeDetector.in(getTime("2013-01-04 11:02:00"));
        Assert.assertEquals(3 * ONE_HOUR, daysStayTimeDetector.getStayTime());
        Assert.assertEquals(3 * ONE_HOUR, stayTimeHolder.value); //到达3个小时，更新

        daysStayTimeDetector.in(getTime("2013-01-04 11:03:00"));
        Assert.assertEquals(3 * ONE_HOUR + 1 * ONE_MINUTE, daysStayTimeDetector.getStayTime());
        Assert.assertEquals(3 * ONE_HOUR + 1 * ONE_MINUTE, stayTimeHolder.value); //第一次超过3个小时，更新

    }


    private long getTime(String s) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").parse(s + " +0000").getTime();
    }


    public static void main(String[] args) throws ParseException {
        Calendar instance = Calendar.getInstance();
        instance.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date time = instance.getTime();
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").parse("2013-01-05 09:00:00 +0000");
        ;
    }

} 
