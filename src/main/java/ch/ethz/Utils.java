package ch.ethz;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by marenato on 02.11.15.
 */
public class Utils {

    public static Properties loadProperties(String props) {
        Properties prop = new Properties();
        InputStream in = Utils.class.getClassLoader().getResourceAsStream(props);
        try {
            prop.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }
}
