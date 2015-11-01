package ch.ethz.scantest.kv;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by renatomarroquin on 2015-11-01.
 */
public abstract class Kv {
    /**
     * Initializes KvStores.
     */
    public abstract void initialize();

    /**
     * Closes KvStores.
     */
    public abstract void destroy();

    /**
     * Selects all records from a schema.table
     * @param schema
     * @param table
     * @return
     */
    public abstract long selectAll(String schema, String table);

    public Properties loadProperties(String props) {
        Properties prop = new Properties();
        InputStream in = getClass().getClassLoader().getResourceAsStream(props);
        try {
            prop.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }
}
