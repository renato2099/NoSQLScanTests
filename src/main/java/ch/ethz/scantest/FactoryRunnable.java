package ch.ethz.scantest;


import ch.ethz.scantest.kv.CassandraKv;
import static ch.ethz.scantest.kv.Kv.kvStores;

import ch.ethz.scantest.kv.HBaseKv;
import org.apache.log4j.Logger;

/**
 * Created by marenato on 30.10.15.
 */
public class FactoryRunnable {

    public static Logger LOG = Logger.getLogger(FactoryRunnable.class);

    public static Runnable getRunnable(kvStores kvStore, final long nOps, final long bSize, final long rStart) {
        LOG.debug(String.format("Creating %s loader. [TotalOps] %d. [BatchSize] %d. [RangeStart] %d",
                kvStore.toString(), nOps, bSize, rStart));
        switch(kvStore) {
            case CASSANDRA:
                return CassandraKv.getLoader(nOps, bSize, rStart);
            case HBASE:
                return HBaseKv.getLoader(nOps, bSize, rStart);
            case HYPERTABLE:
                break;
            case VOLDEMORT:
                break;
            case RIAK:
                break;
            default:
                throw new IllegalArgumentException("KeyValue store not supported!");
        }
        return null;
    }

}
