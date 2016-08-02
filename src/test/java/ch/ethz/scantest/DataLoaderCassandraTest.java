package ch.ethz.scantest;

import static ch.ethz.kv.Kv.kvStores.*;

import ch.ethz.kv.CassandraKv;
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
    public void testLoadGetAll() {
        loadKv(CASSANDRA, DEFAULT_OPS, DEFAULT_BATCH);
        long actual = getAll(CassandraKv.CONTAINER, CassandraKv.TABLE_NAME);
        Log.info(String.format("[Scan %s] Expected:%d Found:%d", CASSANDRA.toString(), DEFAULT_OPS, actual));
    }

    @Test
    public void testGetAll() {
        long actual = getAll(CassandraKv.CONTAINER, CassandraKv.TABLE_NAME);
        Log.info(String.format("[Scan %s] Expected:%d Found:%d", CASSANDRA.toString(), DEFAULT_OPS, actual));
    }

    @Test
    public void testLoadPercentage() {
        double p = 0.5;
        loadKv(CASSANDRA, DEFAULT_OPS, DEFAULT_BATCH);
        long actual = getPercentage(CassandraKv.CONTAINER, CassandraKv.TABLE_NAME, p);
        Log.info(String.format("[Scan %s] Expected:%1.2f Found:%d", CASSANDRA.toString(), DEFAULT_OPS*p, actual));
    }

    @Test
    public void testPercentage() {
        double p = 0.5;
        long actual = getPercentage(CassandraKv.CONTAINER, CassandraKv.TABLE_NAME, p);
        Log.info(String.format("[Scan %s] Expected:%1.2f Found:%d", CASSANDRA.toString(), DEFAULT_OPS*p, actual));
    }

}
