package ch.ethz.scantest;

import static ch.ethz.scantest.FactoryRunnable.kvStores.*;

import ch.ethz.scantest.kv.CassandraKv;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by marenato on 30.10.15.
 */
public class DataLoaderTest {

    private static final long DEFAULT_BATCH = 10;
    private static Logger LOG = Logger.getLogger(DataLoaderTest.class);
    private static final long DEFAULT_OPS = 100;

    @Before
    public void setUp() {
        //todo maybe changing this?
        CassandraKv kv = new CassandraKv();
        kv.initialize();
    }

    @Test
    public void testCassandra() {
        DataLoader dl = new DataLoader();
        long startTime = System.nanoTime();
        long nOps = DEFAULT_OPS;
        long bSize = DEFAULT_BATCH;
        dl.load(nOps, CASSANDRA, bSize);
        long endTime = System.nanoTime();
        LOG.info(String.format("[LoadOp] Elapsed:%d", (endTime - startTime) / 1000));
    }

    @After
    public void destroy() {
//        CassandraKv kv = new CassandraKv();
//        kv.destroy();
    }
}
