package Util;

/**
 * The interface hold constants.
 * @author ashanw
 */
public interface IConstants {
    String TWITTER_PROPERTIES = "twitter.properties";
    String KAFKA_PROPERTIES = "kafka.properties";
    String ELASTICSEARCH_PROPERTIES = "elasticsearch.properties";
    String JOB_NAME = "twitter-streaming";

    interface ElasticSearch {
        String CREATED_AT = "created_at";
        String SENTIMENT_SCORE = "sentiment_score";
        String TWEET = "tweet";
        String LANGUAGE = "language";
        String LOCATION = "location";
    }
}