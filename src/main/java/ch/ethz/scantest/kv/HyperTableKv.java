package ch.ethz.scantest.kv;

import org.apache.thrift.TException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.ColumnFamilySpec;
import org.hypertable.thriftgen.NamespaceListing;
import org.hypertable.thriftgen.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ch.ethz.scantest.kv.Kv.kvStores.HYPERTABLE;

/**
 * Created by renatomarroquin on 2015-11-02.
 */
public class HyperTableKv implements Kv {
    public static final String CONTAINER = "scanks";
    public static final String TABLE_NAME = "employees";
    private static final String NS_LOC = "/scanks_ns/";
    private static ThriftClient client = null;
    private static long ns;

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
        try {
            client = ThriftClient.create("localhost", 15867);
            if (!client.namespace_exists(CONTAINER))
                client.namespace_create(CONTAINER);
            ns = client.namespace_open(CONTAINER);

            boolean if_exists = true;
            client.table_drop(ns, TABLE_NAME, if_exists);

            Schema schema = new Schema();

            Map column_families = new HashMap();

            ColumnFamilySpec cf = new ColumnFamilySpec();
            cf.setName("genus");
            column_families.put("genus", cf);

            cf = new ColumnFamilySpec();
            cf.setName("description");
            column_families.put("description", cf);

            cf = new ColumnFamilySpec();
            cf.setName("tag");
            column_families.put("tag", cf);

            schema.setColumn_families(column_families);

            client.table_create(ns, TABLE_NAME, schema);

            client.namespace_create(NS_LOC);

            List<NamespaceListing> listing;

            listing = client.namespace_get_listing(ns);

            for (NamespaceListing entry : listing)
                System.out.println(entry);


        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    @Override
    public void destroy() {
        try {
            client.namespace_close(ns);
        } catch (TException e) {
            e.printStackTrace();
        }
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
