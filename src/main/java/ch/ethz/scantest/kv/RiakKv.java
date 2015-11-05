package ch.ethz.scantest.kv;

import ch.ethz.scantest.DataGenerator;
import ch.ethz.scantest.Utils;
import com.basho.riak.client.api.RiakClient;

import com.basho.riak.client.api.commands.kv.ListKeys;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.mapreduce.BucketMapReduce;
import com.basho.riak.client.api.commands.mapreduce.MapReduce;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.MapReduceOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.ops.MapOp;
import com.basho.riak.client.core.query.functions.Function;
import com.basho.riak.client.core.util.BinaryValue;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static ch.ethz.scantest.kv.Kv.kvStores.CASSANDRA;
import static ch.ethz.scantest.kv.Kv.kvStores.RIAK;

/**
 * Created by renatomarroquin on 2015-11-02.
 */
public class RiakKv implements Kv {
    public static final String CONTAINER = "scanks";
    public static final String TABLE_NAME = "employees";
    private static final String RIAK_PROPS = "riak.properties";
    private static RiakClient client;
    public static Logger Log = Logger.getLogger(RiakKv.class);
    private String rNodes;
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
            // Riak Client with supplied IP and Port
            Properties props = Utils.loadProperties(RIAK_PROPS);
            rNodes = props.getProperty("nodes");
            port = props.getProperty("port");
            Log.info(String.format("[Load %s] Connected to %s:%s", CASSANDRA.toString(), rNodes,port));
            List<String> ip = new ArrayList<>();
            for (String node : rNodes.split(","))
                ip.add(node);
            client = RiakClient.newClient(Integer.valueOf(port), ip);
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
        Namespace ns = new Namespace("default", TABLE_NAME);
        try {
            String bName = ns.getBucketNameAsString();
            String bType = ns.getBucketTypeAsString();

            //TODO this needs to be redone
            String query = "{\"inputs\":[[\"" + bName + "\",\"p1\",\"\",\"" + bType + "\"]," +
                    "[\"" + bName + "\",\"p2\",\"\",\"" + bType + "\"]," +
                    "[\"" + bName + "\",\"p3\",\"\",\"" + bType + "\"]]," +
                    "\"query\":[{\"map\":{\"language\":\"javascript\",\"source\":\"" +
//                    "function(v) {return v;}" +
                    "function(v) {var m = v.values[0].data.toLowerCase().match(/\\w*/g); var r = [];" +
                    "for(var i in m) {if(m[i] != '') {var o = {};o[m[i]] = 1;r.push(o);}}return r;}" +
                    "\"}},"
                    + "{\"reduce\":{\"language\":\"javascript\",\"source\":\"" +
                    "function(v) {var r = {};for(var i in v) {for(var w in v[i]) {if(w in r) r[w] += v[i][w];" +
                    "else r[w] = v[i][w];}}return [r];}\"}}]}"
                    ;

            MapReduceOperation mrOp =
                    new MapReduceOperation.Builder(BinaryValue.unsafeCreate(query.getBytes()))
                            .build();

            RiakNode.Builder builder = new RiakNode.Builder()
                    .withRemotePort(8087);
            RiakCluster cluster = new RiakCluster.Builder(builder.build()).build();
            cluster.start();
            RiakFuture<MapReduceOperation.Response, BinaryValue> resp = cluster.execute(mrOp);
            MapReduceOperation.Response response = resp.get();
            for (Map.Entry ent :response.getResults().entrySet()) {
                System.out.print(ent.getKey() + ":" + ent.getValue());
            }
            System.out.println(response.getResults().entrySet().size());
            mrOp.await();
            System.out.println(mrOp.isSuccess());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public String getType() {
        return RIAK.toString();
    }
}
