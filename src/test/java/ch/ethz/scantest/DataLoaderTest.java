package ch.ethz.scantest;

import ch.ethz.scantest.kv.CassandraKv;
import ch.ethz.scantest.kv.Kv;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * Created by marenato on 02.11.15.
 */
public class DataLoaderTest {
    public static final long DEFAULT_BATCH = 10;
    public static final long DEFAULT_OPS = 100;
    public static Kv kv;
    private static Logger LOG = Logger.getLogger(DataLoaderTest.class);

    public void getAll(String schName, String tabName) {
        long startTime = System.nanoTime();
        kv.selectAll(CassandraKv.CONTAINER, CassandraKv.TABLE_NAME);
        long endTime = System.nanoTime();
        LOG.info(String.format("[Scan %s] Elapsed:%d", kv.getType(), (endTime - startTime) / 1000));
    }

    public void loadKv(Kv.kvStores kvType, long nOps, long bSize) {
        DataLoader dl = new DataLoader();
        long startTime = System.nanoTime();
        dl.load(nOps, kvType, bSize);
        long endTime = System.nanoTime();
        LOG.info(String.format("[Load %s] Elapsed:%d", kvType.toString() ,(endTime - startTime) / 1000));

    }

    @After
    public void destroy() {
        kv.destroy();
    }
}
