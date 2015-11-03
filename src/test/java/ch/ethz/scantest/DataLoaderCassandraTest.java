package ch.ethz.scantest;

import static ch.ethz.scantest.kv.Kv.kvStores.*;

import ch.ethz.scantest.kv.CassandraKv;
import ch.ethz.scantest.kv.HBaseKv;
import ch.ethz.scantest.kv.Kv;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by marenato on 30.10.15.
 */
public class DataLoaderCassandraTest extends DataLoaderTest {

    @Before
    public void setUp() {
        kv = new CassandraKv();
        kv.initialize();
    }

    @Test
    public void testGetAll() {
        loadKv(CASSANDRA, DEFAULT_OPS, DEFAULT_BATCH);
        long actual = getAll(CassandraKv.CONTAINER, CassandraKv.TABLE_NAME);
        Log.info(String.format("[Scan %s] Expected:%d Found:%d", CASSANDRA.toString(), DEFAULT_OPS, actual));
    }

    @Test
    public void testPercentage() {
        double p = 0.5;
        loadKv(CASSANDRA, DEFAULT_OPS, DEFAULT_BATCH);
        long actual = getPercentage(CassandraKv.CONTAINER, CassandraKv.TABLE_NAME, p);
        Log.info(String.format("[Scan %s] Expected:%1.2f Found:%d", CASSANDRA.toString(), DEFAULT_OPS*p, actual));
    }

}
