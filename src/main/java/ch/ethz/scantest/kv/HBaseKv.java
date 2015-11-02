package ch.ethz.scantest.kv;

/**
 * Created by marenato on 02.11.15.
 */
import ch.ethz.scantest.DataGenerator;
import ch.ethz.scantest.Utils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
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
    public static HTable hTable;
    protected static Configuration hbaseConf;

    public static Runnable getLoader(final long nOps, final long bSize, final long rStart) {
        return new Runnable() {

            @Override
            public void run() {
                long nBatch = nOps / bSize;
                long idStart = rStart;
                try {
                    for (int i = 1; i <= nBatch; i++) {
                        // generate statement of size bSize
                        getBatch(bSize, idStart);
                        // commit batch
                        hTable.flushCommits();
                        idStart += bSize;
                    }
                    // execute remaining
                    if (nOps - (nBatch * bSize) > 0) {
                        getBatch(nOps - (nBatch * bSize), idStart);
                        hTable.flushCommits();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private static void getBatch(long bSize, long idStart) throws IOException {
        DataGenerator dGen = new DataGenerator();
        for (int j = 1; j <= bSize; j++) {
            Put put = new Put(Bytes.toBytes(idStart));
            put.addColumn(Bytes.toBytes(TABLE_NAME), Bytes.toBytes("last"), Bytes.toBytes(dGen.genText(15)));
            put.addColumn(Bytes.toBytes(TABLE_NAME), Bytes.toBytes("first"), Bytes.toBytes(dGen.genText(15)));
            put.addColumn(Bytes.toBytes(TABLE_NAME), Bytes.toBytes("salary"), Bytes.toBytes(dGen.genDouble()));
            put.addColumn(Bytes.toBytes(TABLE_NAME), Bytes.toBytes("service_yrs"), Bytes.toBytes(dGen.genInt()));
            put.addColumn(Bytes.toBytes(TABLE_NAME), Bytes.toBytes("country"), Bytes.toBytes(dGen.getCountry()));
            hTable.put(put);
        }
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
    public void initialize() {
        Properties props = Utils.loadProperties(HBASE_PROPS);
        String cNode = props.getProperty("entry_node");
        String port = props.getProperty("port");
        Log.info(cNode + port);
        try {
            hTable = new HTable(hbaseConf, CONTAINER);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroy() {
        try {
            hTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long selectAll(String schema, String table) {
        Scan scan = new Scan();
        long cnt = 0;
        try {
            ResultScanner resScanner = hTable.getScanner(scan);
            Result next = resScanner.next();
            while (next != null) {
                cnt++;
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
