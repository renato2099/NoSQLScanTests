package ch.ethz.scantest;

import static ch.ethz.scantest.FactoryRunnable.kvStores.*;


/**
 * Created by marenato on 30.10.15.
 */
public class DataLoaderTest {


    private static final long DEFAULT_OPS = 1000;


    public void testCassandra() {
        DataLoader dl = new DataLoader();
        long startTime = System.nanoTime();
        long nOps = DEFAULT_OPS;
        dl.load(nOps, CASSANDRA);
        long endTime = System.nanoTime();
    }
}
