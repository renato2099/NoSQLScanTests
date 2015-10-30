package ch.ethz.scantest;

/**
 * Created by marenato on 30.10.15.
 */
public class FactoryRunnable {

    public static Runnable getRunnable(kvStores kvStore) {
        switch(kvStore) {
            case CASSANDRA:
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
        return new Runnable() {
            @Override
            public void run() {

            }
        };
    }

    public static enum kvStores {
        CASSANDRA("cassandra"), HBASE("hbase"), HYPERTABLE("hypertable"), VOLDEMORT("voldemort"), RIAK("riak");
        private String val;
        kvStores(String v) {
            this.val = v;
        }
    }
}
