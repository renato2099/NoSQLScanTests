package ch.ethz.scantest.kv;

import static ch.ethz.scantest.kv.Kv.kvStores.*;

import ch.ethz.scantest.DataGenerator;
import ch.ethz.scantest.Utils;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.log4j.Logger;

import java.util.Properties;


/**
 * Created by renatomarroquin on 2015-11-01.
 */
public class CassandraKv implements Kv {

    private static final int HIGHER_TIMEOUT = 3600000;
    private static final String CASSANDRA_PROPS = "cassandra.properties";
    public static final String CONTAINER = "scanks";
    private static final String REPL_FACTOR = "1";
    public static final String TABLE_NAME = "employees";
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

    public static Runnable getLoader(final long nOps, final long bSize, final long rStart) {
        return new Runnable() {
            private final Session session = cluster.connect(CONTAINER);

            @Override
            public void run() {
                long nBatch = nOps/bSize;
                Batch batch;
                long idStart = rStart;
                Log.info(String.format("[Load %s] Range %d tuples.", CASSANDRA.toString(), rStart));
                for (int i = 1; i <= nBatch; i++) {
                    // generate statement of size bSize
                    batch = getBatch(bSize, idStart);
                    // commit batch
                    session.execute(batch);
                    idStart += bSize;
                    if (idStart % 1000000 == 1)
                        Log.info(String.format("[Load %s] Inserted %d tuples.", CASSANDRA.toString(), idStart));
                }

                // execute remaining
                if (nOps - (nBatch*bSize) > 0) {
                    batch = getBatch(nOps - (nBatch*bSize), idStart);
                    session.execute(batch);
                }
            }
        };
    }

    private static Batch getBatch(long bSize, long idStart) {
        Batch batch = QueryBuilder.batch();
        DataGenerator dGen = new DataGenerator();
        StringBuilder sb = new StringBuilder();
        for (int j = 1; j <= bSize; j++) {
            RegularStatement insert = QueryBuilder.insertInto(TABLE_NAME).values(
                    new String[] { "id", "last", "first", "salary", "service_yrs", "country" },
                    new Object[] { idStart, dGen.genFixedText(LAST_NAME), dGen.genFixedText(FIRST_NAME), dGen.genDouble(), dGen.genInt(), dGen.getCountry() });
            // is this the right way to set consistency level for Batch?
            insert.setConsistencyLevel(ConsistencyLevel.ANY);
            sb.append(" ").append(idStart);
            idStart++;
            batch.add(insert);
        }
        return batch;
    }

    @Override
    public long selectAll(String keyspace, String table) {
        Session session = cluster.connect();
        Select query = QueryBuilder.select().all().from(CONTAINER, TABLE_NAME);
        Log.info(String.format("[Scan %s] Scanning %s.%s", CASSANDRA.toString(), CONTAINER, TABLE_NAME));
        return session.execute(query).all().size();
    }

    @Override
    public long select(String schema, String table, double percent) {
        Session session = cluster.connect();
//        Clause salary = QueryBuilder.lt("salary", percent);
        //Statement stmt = QueryBuilder.select().all().from(CONTAINER, TABLE_NAME).where(salary).allowFiltering();
        String stmtt = String.format("select * from scanks.employees where salary < %1.4f allow filtering;", percent);
//        return session.execute(stmt).all().size();
        return session.execute(stmtt).all().size();
    }

    @Override
    public String getType() {
        return CASSANDRA.toString();
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
        Properties props = Utils.loadProperties(CASSANDRA_PROPS);
        String cNode = props.getProperty("entry_node");
        String port = props.getProperty("port");
        Log.info(String.format("[Load %s] Connected to %s:%s", CASSANDRA.toString(), cNode,port));
        this.connect(cNode, port);
        this.createKeySpace();
    }

    private void createKeySpace() {
        Session session = cluster.connect();
        // create keyspace
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE KEYSPACE ").append(CONTAINER).append(" WITH replication ");
        sb.append("= {'class':'SimpleStrategy', 'replication_factor':");
        sb.append(REPL_FACTOR).append("}");
        sb.append("AND DURABLE_WRITES = false").append(";");
        try {
            session.execute(sb.toString());
            Log.warn(String.format("[LoadOp %s] KeySpace created!", CASSANDRA.toString()));
        } catch (com.datastax.driver.core.exceptions.AlreadyExistsException ex) {
            Log.warn("[LoadOp] Keyspace already exists!");
        }

        // create table
        sb.setLength(0);
        sb.append("CREATE TABLE ").append(CONTAINER).append(".").append(TABLE_NAME);
        sb.append("(").append("id bigint,");
        sb.append("last varchar,").append("first varchar,");
        sb.append("salary double,").append("service_yrs int,");
        sb.append("country varchar");
        sb.append(",").append("PRIMARY KEY ((id), salary)");
        sb.append(");");

        try {
            session.execute(sb.toString());
            Log.warn(String.format("[LoadOp %s] Table created!", CASSANDRA.toString()));
        } catch (com.datastax.driver.core.exceptions.AlreadyExistsException ex) {
            Log.warn(String.format("[LoadOp %s] Table already exists!", CASSANDRA.toString()));
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
