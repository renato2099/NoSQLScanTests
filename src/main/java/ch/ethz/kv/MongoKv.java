package ch.ethz.kv;

import ch.ethz.scan.QueryBroker;

/**
 * Created by renatomarroquin on 2016-08-02.
 */
public class MongoKv implements Kv {
    @Override
    public String getTableName() {
        return null;
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
    public long scan(String key, String col, QueryBroker.RangeOp qScanOp, Object value) {
        return 0;
    }

    @Override
    public String getType() {
        return kvStores.MONGO.toString();
    }
}
