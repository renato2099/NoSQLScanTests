package ch.ethz.scantest.kv;

import ch.ethz.scantest.DataGenerator;
import ch.ethz.scantest.Utils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.*;

import java.io.IOException;
import java.util.*;

import static ch.ethz.scantest.kv.Kv.kvStores.HYPERTABLE;

/**
 * Created by renatomarroquin on 2015-11-02.
 */
public class HyperTableKv implements Kv {
    public static final String CONTAINER = "scanks";
    public static final String TABLE_NAME = "employees";
    private static final String NS_LOC = "/scanks_ns/";
    private static final String HYPERTABLE_PROPS = "hypertable.properties";
    private static ThriftClient client = null;
    private static long ns;
    public static Logger Log = Logger.getLogger(HyperTableKv.class);
    private String hNode;
    private String port;

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    @Override
    public String getContainerName() {
        return CONTAINER;
    }

    @Override
    public void initialize() {
        try {
            Properties props = Utils.loadProperties(HYPERTABLE_PROPS);
            hNode = props.getProperty("node");
            port = props.getProperty("port");

            client = ThriftClient.create(hNode, Integer.valueOf(port));
            if (!client.namespace_exists(CONTAINER))
                client.namespace_create(CONTAINER);
            ns = client.namespace_open(CONTAINER);

            boolean if_exists = true;
            client.table_drop(ns, CONTAINER, if_exists);

            // creating schema
            Schema schema = new Schema();
            // creating column families
            schema.setColumn_families(getColumnFamily());

            client.table_create(ns, CONTAINER, schema);
            client.namespace_create(NS_LOC);

            List<NamespaceListing> listing;
            listing = client.namespace_get_listing(ns);

            for (NamespaceListing entry : listing)
                Log.warn(String.format("[Load %s] Namespace %s created. ", HYPERTABLE.toString(), entry.getName()));

        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    public Map getColumnFamily() {
        Map cfs = new HashMap();

        ColumnFamilySpec cf = new ColumnFamilySpec();
        cf.setName(TABLE_NAME);
        cfs.put(TABLE_NAME, cf);

//        cf = new ColumnFamilySpec();
//        cf.setName("last");
//        cfs.put("last", cf);
//
//        cf = new ColumnFamilySpec();
//        cf.setName("first");
//        cfs.put("first", cf);
//
//        cf = new ColumnFamilySpec();
//        cf.setName("salary");
//        cfs.put("salary", cf);
//
//        cf = new ColumnFamilySpec();
//        cf.setName("service_yrs");
//        cfs.put("service_yrs", cf);
//
//        cf = new ColumnFamilySpec();
//        cf.setName("country");
//        cfs.put("country", cf);
        return cfs;
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

    public static Runnable getLoader(final long nOps, final long bSize, final long rStart) {
        return new Runnable() {
            @Override
            public void run() {
                long nBatch = nOps / bSize;
                long idStart = rStart;
                try {
                    for (int i = 1; i <= nBatch; i++) {
                        // generate statement of size bSize
                        getBatch(bSize, idStart, ns);
                        idStart += bSize;
                    }
                    // execute remaining
                    if (nOps - (nBatch * bSize) > 0) {
                        getBatch(nOps - (nBatch * bSize), idStart, ns);
                    }
                } catch (TException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private static List getBatch(long bSize, long idStart, long ns) throws TException {
        List cells = new ArrayList();
        DataGenerator dGen = new DataGenerator();
        Key key = null;
        for (int j = 1; j <= bSize; j++) {
            key = new Key();
            key.setRow(String.valueOf(idStart));
            key.setColumn_family(TABLE_NAME);
            client.set_cells(ns, TABLE_NAME, buildRecord(key, dGen));
            idStart ++;
        }
        return cells;
    }

    private static List buildRecord(Key k, DataGenerator dGen) {

        List cells = new ArrayList();
        Cell cell = new Cell();
        k.setColumn_qualifier("l");
        cell.setKey(k);
        cell.setValue(Bytes.toBytes(dGen.genText(15)));
        cells.add(cell);

        cell = new Cell();
        k.setColumn_qualifier("f");
        cell.setKey(k);
        cell.setValue(Bytes.toBytes(dGen.genText(15)));
        cells.add(cell);

        k.setColumn_qualifier("s");
        cell.setKey(k);
        cell.setValue(Bytes.toBytes(dGen.genDouble()));
        cells.add(cell);

        k.setColumn_qualifier("sy");
        cell.setKey(k);
        cell.setValue(Bytes.toBytes(dGen.genInt()));
        cells.add(cell);

        k.setColumn_qualifier("c");
        cell.setKey(k);
        cell.setValue(Bytes.toBytes(dGen.getCountry()));
        cells.add(cell);

        return cells;
    }
}
