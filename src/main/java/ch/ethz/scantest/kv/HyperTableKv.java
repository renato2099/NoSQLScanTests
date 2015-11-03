package ch.ethz.scantest.kv;

import static ch.ethz.scantest.kv.Kv.kvStores.HYPERTABLE;

/**
 * Created by renatomarroquin on 2015-11-02.
 */
public class HyperTableKv implements Kv {
    @Override
    public String getTableName() {
        return HYPERTABLE.toString();
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
        return HYPERTABLE.toString();
    }

    public static Runnable getLoader(long nOps, long bSize, long rStart) {
        return new Runnable() {
            @Override
            public void run() {

            }
        };
    }
}
