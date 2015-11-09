package ch.ethz.scantest;

import ch.ethz.scantest.kv.Kv;
import org.apache.log4j.Logger;
import org.junit.After;

/**
 * Created by marenato on 02.11.15.
 */
public class DataLoaderTest {
    public static final long DEFAULT_BATCH = 10;
    public static final long DEFAULT_OPS = 120000000;
    public static final int DEFAULT_NTHREADS = 12;
    public static Kv kv;
    public static Logger Log = Logger.getLogger(DataLoaderTest.class);

    public long getPercentage(String container, String tableName, double percentage) {
        long startTime = System.nanoTime();
        long actual = kv.select(kv.getContainerName(), kv.getTableName(), percentage);
        long endTime = System.nanoTime();
        Log.info(String.format("[Scan %s] Elapsed:%d", kv.getType().toString(), (endTime - startTime) / 1000));
        return actual;
    }

    public long getAll(String schName, String tabName) {
        long startTime = System.nanoTime();
        long actual = kv.selectAll(kv.getContainerName(), kv.getTableName());
        long endTime = System.nanoTime();
        Log.info(String.format("[Scan %s] Elapsed:%d", kv.getType(), (endTime - startTime) / 1000));
        return actual;
    }

    public void loadKv(Kv.kvStores kvType, long nOps, long bSize) {
        DataLoader dl = new DataLoader(DEFAULT_NTHREADS);
        long startTime = System.nanoTime();
        dl.load(nOps, kvType, bSize);
        long endTime = System.nanoTime();
        Log.info(String.format("[Load %s] Elapsed:%d", kvType.toString(), (endTime - startTime) / 1000));
    }

    @After
    public void destroy() {
//        kv.destroy();
    }
}
