package Util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * The class stores utility methods to read property files.
 * @author ashanw
 * @since 3-4-2020
 */
public class PropertyFile {

    public static Properties getKafkaProperties() throws IOException {
        Properties kafkaProperties = new Properties();
        kafkaProperties.load(new FileInputStream(IConstants.KAFKA_PROPERTIES));
        return kafkaProperties;
    }

    public static Properties getTwitterProperties() throws IOException {
        Properties twitterProperties = new Properties();
        twitterProperties.load(new FileInputStream(IConstants.TWITTER_PROPERTIES));
        return twitterProperties;
    }

    public static Properties getElasticSearchProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(IConstants.ELASTICSEARCH_PROPERTIES));
        return properties;
    }
}
