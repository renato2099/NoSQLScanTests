package ch.ethz.scantest.kv;

import ch.ethz.scantest.DataGenerator;
import ch.ethz.scantest.Utils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
            if (!client.namespace_exists(NS_LOC))
                client.namespace_create(NS_LOC);
            ns = client.namespace_open(NS_LOC);

            boolean if_exists = true;
            client.table_drop(ns, CONTAINER, if_exists);

            // creating schema
            Schema schema = new Schema();
            // creating column families
            schema.setColumn_families(getColumnFamily());

            client.table_create(ns, CONTAINER, schema);
//            client.namespace_create(NS_LOC);

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
        return cfs;
    }

    @Override
    public void destroy() {
        try {
            //todo drop table/namespace
            client.namespace_close(ns);
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long selectAll(String schema, String table) {
        ScanSpec ss = new ScanSpec();
        List columns = new ArrayList();
        columns.add(table);
        ss.setColumns(columns);
        long size = 0;
        try {
            HqlResult result = client.hql_exec(ns, "SELECT * from " + CONTAINER, false, true);
            while (!client.scanner_get_row_as_arrays(result.scanner).isEmpty())
                size ++;
            client.scanner_close(result.scanner);
        } catch (TException e) {
            e.printStackTrace();
        }

        return size;
    }

    @Override
    public long select(String schema, String table, double percent) {
        ScanSpec ss = new ScanSpec();
        List columns = new ArrayList();
        columns.add(table);
        ss.setColumns(columns);
        long size = 0;
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT * from ").append(CONTAINER).append(" WHERE value regexp ");
            sb.append("\"^(?:[1-9]\\d*|0)?(?:\\.[0-").append((int) (percent * 10)).append("]\\d+)?$\"");
            HqlResult result = client.hql_exec(ns,  sb.toString(), false, true);
            while (!client.scanner_get_row_as_arrays(result.scanner).isEmpty())
                size ++;
            client.scanner_close(result.scanner);
        } catch (TException e) {
            e.printStackTrace();
        }
        return size;
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
            client.set_cells(ns, CONTAINER, buildRecord(String.valueOf(idStart), dGen));
            idStart ++;
        }
        return cells;
    }

    private static List buildRecord(String idStart, DataGenerator dGen) {

        List cells = new ArrayList();
        Cell cell = new Cell();
        Key k = new Key();
        k.setRow(idStart);
        k.setColumn_family(TABLE_NAME);
        k.setColumn_qualifier("l");
        cell.setKey(k);
        cell.setValue(Bytes.toBytes(dGen.genText(15)));
        cells.add(cell);

        k = new Key();
        k.setRow(idStart);
        k.setColumn_family(TABLE_NAME);
        k.setColumn_qualifier("f");
        cell = new Cell();
        cell.setKey(k);
        cell.setValue(Bytes.toBytes(dGen.genText(15)));
        cells.add(cell);

        k = new Key();
        k.setRow(idStart);
        k.setColumn_family(TABLE_NAME);
        k.setColumn_qualifier("s");
        cell = new Cell();
        cell.setKey(k);
        cell.setValue(Bytes.toBytes(String.valueOf(dGen.genDouble())));
        cells.add(cell);

        k = new Key();
        k.setRow(idStart);
        k.setColumn_family(TABLE_NAME);
        k.setColumn_qualifier("sy");
        cell = new Cell();
        cell.setKey(k);
        cell.setValue(Bytes.toBytes(dGen.genInt()));
        cells.add(cell);

        k = new Key();
        k.setRow(idStart);
        k.setColumn_family(TABLE_NAME);
        k.setColumn_qualifier("c");
        cell = new Cell();
        cell.setKey(k);
        cell.setValue(Bytes.toBytes(dGen.getCountry()));
        cells.add(cell);

        return cells;
    }
}
