package ch.ethz.scantest;

import ch.ethz.kv.HyperTableKv;
import org.junit.Before;
import org.junit.Test;

import static ch.ethz.kv.Kv.kvStores.HYPERTABLE;

/**
 * Created by renatomarroquin on 2015-11-03.
 */
public class DataLoaderHyperTableTest extends DataLoaderTest {
    @Before
    public void setUp() {
        kv = new HyperTableKv();
        kv.initialize();
    }

    @Test
    public void testGetAll() {
        loadKv(HYPERTABLE, DEFAULT_OPS, DEFAULT_BATCH);
        long actual = getAll(HyperTableKv.CONTAINER, HyperTableKv.TABLE_NAME);
        Log.info(String.format("[Scan %s] Expected:%d Found:%d", HYPERTABLE.toString(), DEFAULT_OPS, actual));
    }

    @Test
    public void testPercentage() {
        double p = 0.5;
        loadKv(HYPERTABLE, DEFAULT_OPS, DEFAULT_BATCH);
        long actual = getPercentage(HyperTableKv.CONTAINER, HyperTableKv.TABLE_NAME, p);
        Log.info(String.format("[Scan %s] Expected:%1.2f Found:%d", HYPERTABLE.toString(), DEFAULT_OPS*p, actual));
    }
}
