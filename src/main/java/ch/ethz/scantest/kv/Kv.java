package ch.ethz.scantest.kv;

/**
 * Created by renatomarroquin on 2015-11-01.
 */
public interface Kv {
    enum kvStores {
        CASSANDRA("cassandra"), HBASE("hbase"), HYPERTABLE("hypertable"), RIAK("riak");
        private String val;
        kvStores(String v) {
            this.val = v;
        }
    }

    /**
     * Gets table/column family name
     * @return
     */
    String getTableName();

    /**
     * Gets keyspace/table
     * @return
     */
    String getContainerName();

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
     * Selects a percentage of the table.
     * @param schema
     * @param table
     * @param percent
     * @return
     */
    long select(String schema, String table, double percent);

    /**
     * Gets a kv type
     * @return
     */
    String getType();

}
