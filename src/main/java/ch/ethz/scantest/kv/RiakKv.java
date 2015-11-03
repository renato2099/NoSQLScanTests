package ch.ethz.scantest.kv;

import ch.ethz.scantest.DataGenerator;
import com.basho.riak.client.api.RiakClient;

import com.basho.riak.client.api.commands.kv.ListKeys;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static ch.ethz.scantest.kv.Kv.kvStores.RIAK;

/**
 * Created by renatomarroquin on 2015-11-02.
 */
public class RiakKv implements Kv {
    public static final String CONTAINER = "scanks";
    public static final String TABLE_NAME = "employees";
    private static RiakClient client;
    public static Logger Log = Logger.getLogger(RiakKv.class);

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
            // Riak Client with supplied IP and Port
            int port = 8087;
            List<String> ip = new ArrayList<>();
            ip.add("127.0.0.1");
            client = RiakClient.newClient(port, ip);

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }


    public static Runnable getLoader(final long nOps, final long bSize, final long rStart) {
        return new Runnable() {
            Namespace ns = new Namespace("default", TABLE_NAME);
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
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    static class Employee {
        public String first;
        public String last;
        public double salary;
        public int service_yrs;
        public String country;
        public Employee(String f, String l, double s, int sy, String c) {
            this.first = f;
            this.last = l;
            this.salary = s;
            this.service_yrs = sy;
            this.country = c;
        }
    }

    private static void getBatch(long bSize, long idStart, Namespace ns) throws IOException {
        DataGenerator dGen = new DataGenerator();
        try {
            for (int j = 1; j <= bSize; j++) {
                Location loc = new Location(ns, BinaryValue.create(Bytes.toBytes(idStart)));
                Employee obj = new Employee(dGen.genText(15), dGen.genText(15),
                        dGen.genDouble(), dGen.genInt(), dGen.getCountry());
                StoreValue storeWithProps = new StoreValue.Builder(obj)
                        .withLocation(loc).build();
                StoreValue.Response storeWithPropsResp = client.execute(storeWithProps);
                // commit batch
                if (!storeWithPropsResp.hasGeneratedKey())
                    Log.warn(String.format("[Load %s] Key %d not inserted", RIAK.toString(), idStart));
                idStart ++;
            }
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroy() {
        client.shutdown();
    }

    @Override
    public long selectAll(String schema, String table) {
        Namespace ns = new Namespace("default", TABLE_NAME);
        ListKeys lk = new ListKeys.Builder(ns).build();
        ListKeys.Response response = null;
        long cnt = 0;
        try {
            response = client.execute(lk);
            for (Location l : response)
                cnt ++;
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return cnt;
    }

    @Override
    public long select(String schema, String table, double percent) {
        return 0;
    }

    @Override
    public String getType() {
        return RIAK.toString();
    }
}
