package ch.ethz.scantest.kv;

import ch.ethz.scantest.DataGenerator;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.log4j.Logger;

import java.util.Properties;


/**
 * Created by renatomarroquin on 2015-11-01.
 */
public class CassandraKv extends Kv {

    private static final int HIGHER_TIMEOUT = 10000;
    private static final String CASSANDRA_PROPS = "cassandra.properties";
    private static final String CONTAINER = "scanks";
    private static final String REPL_FACTOR = "1";
    private static final String TABLE_NAME = "employees";
    private static Cluster cluster;

    public static Logger Log = Logger.getLogger(CassandraKv.class);

    public void connect(String node, String port) {
        cluster = Cluster.builder().withPort(Integer.parseInt(port))
                .addContactPoint(node).build();
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(HIGHER_TIMEOUT);
        Metadata md = cluster.getMetadata();
        Log.info(String.format("Connected to: %s\n", md.getClusterName()));
        for ( Host h : md.getAllHosts() ) {
            Log.debug(String.format("Datatacenter: %s; Host: %s; Rack: %s\n",
                    h.getDatacenter(), h.getAddress(), h.getRack()));
        }
    }

    public static Runnable getLoader(final long nOps, final long bSize) {
        return new Runnable() {
            private final Session session = cluster.connect(CONTAINER);
            @Override
            public void run() {
                long nBatch = nOps/bSize;
                Batch batch;
                for (int i = 1; i <= nBatch; i++) {
                    // generate statement of size bSize
                    batch = getBatch(bSize, i* Thread.currentThread().getId());
                    // commit batch
                    session.execute(batch);
                }
                // execute remaining
                if (nOps - (nBatch*bSize) > 0) {
                    batch = getBatch(nOps - (nBatch*bSize), nBatch* Thread.currentThread().getId());
                    session.execute(batch);
                }
            }
        };
    }

    private static Batch getBatch(long bSize, long bNum) {
        Batch batch = QueryBuilder.batch();
        DataGenerator dGen = new DataGenerator();
        for (int j = 1; j <= bSize; j++) {
            RegularStatement insert = QueryBuilder.insertInto(TABLE_NAME).values(
                    new String[] { "id", "last", "first", "salary", "service_yrs", "country" },
                    new Object[] { j*bNum, dGen.genText(15), dGen.genText(20), dGen.genDouble(), dGen.genInt(), dGen.getCountry() });
            // is this the right way to set consistency level for Batch?
            insert.setConsistencyLevel(ConsistencyLevel.QUORUM);
            batch.add(insert);
        }
        return batch;
    }

    @Override
    public long selectAll(String keyspace, String table) {
        Session session = cluster.connect();
        Select query = QueryBuilder.select().all().from(CONTAINER, TABLE_NAME);
        return session.execute(query).all().size();
    }

    @Override
    public void initialize() {
        Properties props = loadProperties(CASSANDRA_PROPS);
        String cNode = props.getProperty("entry_node");
        String port = props.getProperty("port");
        Log.info(cNode + port);
        this.connect(cNode, port);
        this.createKeySpace();
    }

    private void createKeySpace() {
        Session session = cluster.connect();
        // create keyspace
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE KEYSPACE ").append(CONTAINER).append(" WITH replication ");
        sb.append("= {'class':'SimpleStrategy', 'replication_factor':");
        sb.append(REPL_FACTOR).append("};");
        try {
            session.execute(sb.toString());
        } catch (com.datastax.driver.core.exceptions.AlreadyExistsException ex) {
            Log.warn("[LoadOp] Keyspace already exists!");
        }

        // create table
        sb.setLength(0);
        sb.append("CREATE TABLE ").append(CONTAINER).append(".").append(TABLE_NAME);
        sb.append("(").append("id bigint PRIMARY KEY,");
        sb.append("last varchar,").append("first varchar,");
        sb.append("salary double,").append("service_yrs int,");
        sb.append("country varchar").append(");");
        try {
            session.execute(sb.toString());
        } catch (com.datastax.driver.core.exceptions.AlreadyExistsException ex) {
            Log.warn("[LoadOp] Table already exists!");
        }

    }


    @Override
    public void destroy() {
        Session session = cluster.connect();
        StringBuilder sb = new StringBuilder();
        sb.append("drop table ").append(CONTAINER).append(".");
        sb.append(TABLE_NAME).append(";");
        session.execute(sb.toString());
        sb.setLength(0);
        sb.append("drop keyspace ").append(CONTAINER).append(";");
        session.execute(sb.toString());
        cluster.close();
    }

}
