package ch.ethz.scantest;

import ch.ethz.scantest.kv.HBaseKv;
import org.junit.Before;
import org.junit.Test;

import static ch.ethz.scantest.kv.Kv.kvStores.*;

/**
 * Created by marenato on 02.11.15.
 */
public class DataLoaderHBaseTest extends DataLoaderTest {
    @Before
    public void setUp() {
        kv = new HBaseKv();
        kv.initialize();
    }

    @Test
    public void testCassandra() {
        loadKv(HBASE, DEFAULT_OPS, DEFAULT_BATCH);
        getAll(HBaseKv.CONTAINER, HBaseKv.TABLE_NAME);
    }
}
