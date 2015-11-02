package ch.ethz.scantest.kv;

import static ch.ethz.scantest.kv.Kv.kvStores.RIAK;

/**
 * Created by renatomarroquin on 2015-11-02.
 */
public class RiakKv implements Kv {
    @Override
    public String getTableName() {
        return RIAK.toString();
    }

    @Override
    public String getContainerName() {
        return null;
    }

    @Override
    public void initialize() {

    }

    @Override
    public void destroy() {

    }

    @Override
    public long selectAll(String schema, String table) {
        return 0;
    }

    @Override
    public long select(String schema, String table, double percent) {
        return 0;
    }

    @Override
    public String getType() {
        return null;
    }
}
