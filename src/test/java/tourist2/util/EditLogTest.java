package tourist2.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

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
    int count = 1000 * 1000;
    long begin = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      AccountSnapshot a = new AccountSnapshot(1356999100000L + i, "100001019584781", 1356999187197L, true, true, 1356999187197L, 1356999187197L, true, new long[10], Accout.Status.Worker);
      editlog.append(a);
    }
    editlog.close();
    System.out.println("write " + (count * 1000 / (System.currentTimeMillis() - begin)) + " per second");
    final AtomicInteger a = new AtomicInteger();
    begin = System.currentTimeMillis();
    editlog.forEachFromTail(new EditLog.RecordProcessor<AccountSnapshot>() {
      @Override
      public boolean on(AccountSnapshot record) {
        int b = a.incrementAndGet();
        if (b % (10 * 1000) == 0) {
          System.out.println(b + ":" + record.getStart());
        }
        return true;
      }
    });
    System.out.println("read " + (a.get() * 1000 / (System.currentTimeMillis() - begin)) + " per second");
    Assert.assertEquals(a.get(), count);
  }

}
