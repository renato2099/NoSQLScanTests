package ch.ethz.scantest;

import static ch.ethz.scantest.kv.Kv.kvStores.*;

import ch.ethz.scantest.kv.CassandraKv;
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
    public void testCassandra() {
        loadKv(CASSANDRA, DEFAULT_OPS, DEFAULT_BATCH);
        getAll(CassandraKv.CONTAINER, CassandraKv.TABLE_NAME);
    }

}
