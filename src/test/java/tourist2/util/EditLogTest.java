package tourist2.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-4-19
 * Time: 下午3:21
 * To change this template use File | Settings | File Templates.
 */
public class EditLogTest {
    @Test
    public void testWriteCountAndReadCount() throws Exception {
        EditLog editlog = new EditLog(new File("e:\\log\\"), AccountSnapshot.class);
        int count = 10 * 1000 * 1000;
        long begin = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            AccountSnapshot a = new AccountSnapshot(1356999100000L + i, "100001019584781", 1356999187197L, true, true, 1356999187197L, 1356999187197L, true, new long[10], Accout.Status.Worker);
            editlog.append(a);
        }
        editlog.close();
        System.out.println("write " + (count * 1000 / (System.currentTimeMillis() - begin)) + " per second");
        final AtomicInteger a = new AtomicInteger();
        begin = System.currentTimeMillis();
        final AtomicLong lastStart = new AtomicLong();
        editlog.forEachFromTail(new EditLog.RecordProcessor<AccountSnapshot>() {
            @Override
            public boolean on(AccountSnapshot record) {
                if (lastStart.get() != 0) {
                    Assert.assertEquals(lastStart.get() - 1, record.getStart());
                }
                lastStart.set(record.getStart());
                a.incrementAndGet();
                return true;
            }
        });
        System.out.println("read " + (a.get() * 1000 / (System.currentTimeMillis() - begin)) + " per second");
        Assert.assertEquals(a.get(), count);
    }

    @Test
    public void testSeek() throws Exception {
        EditLog editlog = new EditLog(new File("e:\\log\\"), AccountSnapshot.class);
        int count = 10 * 1000 * 1000;
        long begin = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            AccountSnapshot a = new AccountSnapshot(1356999100000L + i, "100001019584781", 1356999187197L, true, true, 1356999187197L, 1356999187197L, true, new long[10], Accout.Status.Worker);
            editlog.append(a);
        }
        editlog.close();
        System.out.println("write " + (count * 1000 / (System.currentTimeMillis() - begin)) + " per second");
        final AtomicInteger a = new AtomicInteger();
        begin = System.currentTimeMillis();
        final AtomicLong lastStart = new AtomicLong();
        final List<AccountSnapshot> records = new ArrayList<AccountSnapshot>();
        editlog.forEachFromTail(new EditLog.RecordProcessor<AccountSnapshot>() {
            @Override
            public boolean on(AccountSnapshot record) {
                if (lastStart.get() != 0) {
                    Assert.assertEquals(lastStart.get() - 1, record.getStart());
                }
                lastStart.set(record.getStart());
                int b = a.incrementAndGet();
                if(b>1000 * 1000 * 5){
                    records.add(record);
                }
                if (b > 1000 * 1000 * 5 && record.isSync()) {
//                    records.add(record);
                    return false;
                }
                return true;
            }
        });

        System.out.println(a.get());

        System.out.println("-------------------");
        String r_1 = r(records.get(1));
        String r_2 = r(records.get(2));
        System.out.println(r_2);
        System.out.println(r_1);
        System.out.println(r(records.get(0)));


        AccountSnapshot record = records.get(0);

        editlog.seek(record.getLogNameIndex(),record.getStartPosition());
        AccountSnapshot b = new AccountSnapshot(1000L, "100001019584781", 1356999187197L, true, true, 1356999187197L, 1356999187197L, true, new long[10], Accout.Status.Worker);
        editlog.append(b);
        b = new AccountSnapshot(1001L, "100001019584781", 1356999187197L, true, true, 1356999187197L, 1356999187197L, true, new long[10], Accout.Status.Worker);
        editlog.append(b);
        records.clear();
        editlog.forEachFromTail(new EditLog.RecordProcessor<AccountSnapshot>() {
            @Override
            public boolean on(AccountSnapshot record) {
                records.add(record);
                return records.size() < 5;
            }
        });
        System.out.println("-------------------");
        System.out.println(r(records.get(0)));
        System.out.println(r(records.get(1)));
        System.out.println(r(records.get(2)));
        Assert.assertEquals(r_1,r(records.get(2)));
        System.out.println(r(records.get(3)));
        Assert.assertEquals(r_2,r(records.get(3)));

    }
    private static String r(AccountSnapshot t){
        return "start:"+t.getStart()+",position:"+t.getStartPosition();
    }

    @Test
    public void a(){
        EditLog<AccountSnapshot> editlog = new EditLog<AccountSnapshot>(new File("c:\\log\\"), AccountSnapshot.class);
        AccountSnapshot accountSnapshot = new AccountSnapshot(800L,"arrival1",1213123L,true,true,1L,1L,true,new long[30], Accout.Status.Normal);
        editlog.append(accountSnapshot);
        accountSnapshot = new AccountSnapshot(800L,"arrival1",1213124L,true,true,1L,1L,true,new long[30], Accout.Status.Normal);
        editlog.append(accountSnapshot);
        editlog.close();
        editlog.forEachFromTail(new EditLog.RecordProcessor<AccountSnapshot>() {
            @Override
            public boolean on(AccountSnapshot record) {
                System.out.println("--------------------------------");
                System.out.println("imsi:"+record.getImsi());
                System.out.println("time:"+record.getTime());
                return true;
            }
        });
    }

}
