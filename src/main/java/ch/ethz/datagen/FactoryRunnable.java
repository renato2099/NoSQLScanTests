package ch.ethz.datagen;


import ch.ethz.kv.CassandraKv;
import static ch.ethz.kv.Kv.kvStores;

import ch.ethz.kv.HBaseKv;
import ch.ethz.kv.HyperTableKv;
import ch.ethz.kv.RiakKv;
import org.apache.log4j.Logger;

/**
 * Created by marenato on 30.10.15.
 */
public class FactoryRunnable {

    public static Logger Log = Logger.getLogger(FactoryRunnable.class);

    public static Runnable getRunnable(kvStores kvStore, final long nOps, final long bSize, final long rStart) {
        Log.debug(String.format("Creating %s loader. [TotalOps] %d. [BatchSize] %d. [RangeStart] %d",
                kvStore.toString(), nOps, bSize, rStart));
        switch(kvStore) {
            case CASSANDRA:
                return CassandraKv.getLoader(nOps, bSize, rStart);
            case HBASE:
                return HBaseKv.getLoader(nOps, bSize, rStart);
            case HYPERTABLE:
                return HyperTableKv.getLoader(nOps, bSize, rStart);
            case RIAK:
                return RiakKv.getLoader(nOps, bSize, rStart);
            default:
                throw new IllegalArgumentException("KeyValue store not supported!");
        }
    }

}
