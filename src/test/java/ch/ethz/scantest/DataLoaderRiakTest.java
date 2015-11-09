package ch.ethz.scantest;

import ch.ethz.scantest.kv.RiakKv;
import org.junit.Before;
import org.junit.Test;

import static ch.ethz.scantest.kv.Kv.kvStores.HBASE;
import static ch.ethz.scantest.kv.Kv.kvStores.RIAK;

/**
 * Created by renatomarroquin on 2015-11-02.
 */
public class DataLoaderRiakTest extends DataLoaderTest {
    @Before
    public void setUp() {
        kv = new RiakKv();
        kv.initialize();
    }

    @Test
    public void testGetAll() {
        loadKv(RIAK, DEFAULT_OPS, DEFAULT_BATCH);
        long actual = getAll(RiakKv.CONTAINER, RiakKv.TABLE_NAME);
        Log.info(String.format("[Scan %s] Expected:%d Found:%d", RIAK.toString(), DEFAULT_OPS, actual));
    }

    @Test
    public void testPercentage() {
        double p = 0.5;
        loadKv(RIAK, DEFAULT_OPS, DEFAULT_BATCH);
        long actual = getPercentage(RiakKv.CONTAINER, RiakKv.TABLE_NAME, p);
        Log.info(String.format("[Scan %s] Expected:%1.2f Found:%d", RIAK.toString(), DEFAULT_OPS*p, actual));
    }
}
