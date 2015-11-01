package ch.ethz.scantest;


import ch.ethz.scantest.kv.CassandraKv;
import org.apache.log4j.Logger;

/**
 * Created by marenato on 30.10.15.
 */
public class FactoryRunnable {

    public static Logger LOG = Logger.getLogger(FactoryRunnable.class);
    public static Runnable getRunnable(kvStores kvStore, final long nOps, final long bSize) {
        Runnable loader = null;
        switch(kvStore) {
            case CASSANDRA:
                loader = CassandraKv.getLoader(nOps, bSize);
                break;
            case HBASE:
                break;
            case HYPERTABLE:
                break;
            case VOLDEMORT:
                break;
            case RIAK:
                break;
        }
        return loader;
    }

    public static enum kvStores {
        CASSANDRA("cassandra"), HBASE("hbase"), HYPERTABLE("hypertable"), VOLDEMORT("voldemort"), RIAK("riak");
        private String val;
        kvStores(String v) {
            this.val = v;
        }
    }
}
