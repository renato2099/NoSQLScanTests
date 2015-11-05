package ch.ethz.scantest.kv;

/**
 * Created by marenato on 02.11.15.
 */
import ch.ethz.scantest.DataGenerator;
import ch.ethz.scantest.Utils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Properties;

import static ch.ethz.scantest.kv.Kv.kvStores.*;

/**
 * HBase key value implementation
 */
public class HBaseKv implements Kv {

    public static final String CONTAINER = "scanks";
    public static final String TABLE_NAME = "employees";
    private static final String HBASE_PROPS = "hbase.properties";
    public static Logger Log = Logger.getLogger(HBaseKv.class);
    private static HBaseAdmin admin;
    private static Configuration hbaseConf;

    public static Runnable getLoader(final long nOps, final long bSize, final long rStart) {
        return new Runnable() {

            @Override
            public void run() {
                long nBatch = nOps / bSize;
                long idStart = rStart;
                HTable hTable = null;
                try {
                    hTable = new HTable(hbaseConf, CONTAINER);
                    for (int i = 1; i <= nBatch; i++) {
                        // generate statement of size bSize
                        getBatch(bSize, idStart, hTable);
                        idStart += bSize;
                    }
                    // execute remaining
                    if (nOps - (nBatch * bSize) > 0) {
                        getBatch(nOps - (nBatch * bSize), idStart, hTable);
                    }
                    hTable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private static void getBatch(long bSize, long idStart, HTable hTable) throws IOException {
        DataGenerator dGen = new DataGenerator();
        for (int j = 1; j <= bSize; j++) {
            Put put = new Put(Bytes.toBytes(idStart));
            // last
            put.addColumn(Bytes.toBytes(TABLE_NAME), Bytes.toBytes("l"), Bytes.toBytes(dGen.genText(LAST_NAME)));
            // first
            put.addColumn(Bytes.toBytes(TABLE_NAME), Bytes.toBytes("f"), Bytes.toBytes(dGen.genText(FIRST_NAME)));
            // salary
            put.addColumn(Bytes.toBytes(TABLE_NAME), Bytes.toBytes("s"), Bytes.toBytes(dGen.genDouble()));
            // service_yrs
            put.addColumn(Bytes.toBytes(TABLE_NAME), Bytes.toBytes("sy"), Bytes.toBytes(dGen.genInt()));
            // country
            put.addColumn(Bytes.toBytes(TABLE_NAME), Bytes.toBytes("c"), Bytes.toBytes(dGen.getCountry()));
            idStart ++;
            hTable.put(put);
        }
        // commit batch
        hTable.flushCommits();
    }

    /**
     * Creates a HBaseConf using default parameters
     *
     * @throws IOException
     */
    public static void createHBaseConf() throws IOException {
        hbaseConf = HBaseConfiguration.create();
//        hbaseConf.set("hbase.coprocessor.region.classes",
//                "ch.ethz.scantest.kv.CoprocessorFilter");
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 100 * 1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);

//        final String rootdir = "/tmp/hbase.test.dir/";
//        File rootdirFile = new File(rootdir);
//        if (rootdirFile.exists()) {
//            delete(rootdirFile);
//        }
//        hbaseConf.set("hbase.rootdir", rootdir);
    }

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
        Properties props = Utils.loadProperties(HBASE_PROPS);
        String cNode = props.getProperty("entry_node");
        String port = props.getProperty("port");
        Log.info(cNode + port);
        try {
            createHBaseConf();
            admin = new HBaseAdmin(hbaseConf);
            HTableDescriptor desc = new HTableDescriptor(CONTAINER);
            desc.addFamily(new HColumnDescriptor(TABLE_NAME));
            admin.createTable(desc);
            Log.warn(String.format("[Load %s] Table %s created.", getType(), TABLE_NAME));
        } catch (TableExistsException e) {
            Log.warn(String.format("[Load %s] Table %s exists already!", getType(), TABLE_NAME));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroy() {
        try {
            admin.disableTable(CONTAINER);
            Log.warn(String.format("[Load %s] Table % disabled.", getType(), TABLE_NAME));
            admin.deleteTable(CONTAINER);
            Log.warn(String.format("[Load %s] Table % dropped.", getType(), TABLE_NAME));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long selectAll(String schema, String table) {
        Scan scan = new Scan();
        long cnt = 0;
        try {
            HTable hTable = new HTable(hbaseConf, CONTAINER);
            ResultScanner resScanner = hTable.getScanner(scan);
            Result next = resScanner.next();
            while (next != null) {
                cnt++;
                next = resScanner.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cnt;
    }

    @Override
    public long select(String schema, String table, double percent) {
        Filter colFilter = new SingleColumnValueFilter(Bytes.toBytes(TABLE_NAME), Bytes.toBytes("s"),
                CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(percent));
        Scan scan = new Scan();
        scan.setFilter(colFilter);
        long cnt = 0;
        try {
            HTable hTable = new HTable(hbaseConf, CONTAINER);
            ResultScanner resScanner = hTable.getScanner(scan);
            Result next = resScanner.next();
            while (next != null) {
                cnt++;
                next = resScanner.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cnt;

    }

    @Override
    public String getType() {
        return HBASE.toString();
    }
}
