package util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * The class stores utility methods to read property files.
 * @author ashanw
 * @since 3-4-2020
 */
public class PropertyFile {

    public static Properties getProperties(final String propertiesFile) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(propertiesFile));
        return properties;
    }
}
