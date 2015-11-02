package ch.ethz.scantest.kv;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by renatomarroquin on 2015-11-01.
 */
public interface Kv {
    String getTableName();

    String getContainerName();

    enum kvStores {
        CASSANDRA("cassandra"), HBASE("hbase"), HYPERTABLE("hypertable"), VOLDEMORT("voldemort"), RIAK("riak");
        private String val;
        kvStores(String v) {
            this.val = v;
        }
    }

    /**
     * Initializes KvStores.
     */
    void initialize();

    /**
     * Closes KvStores.
     */
    void destroy();

    /**
     * Selects all records from a schema.table
     * @param schema
     * @param table
     * @return
     */
    long selectAll(String schema, String table);

    /**
     * Gets a kv type
     * @return
     */
    String getType();

}
