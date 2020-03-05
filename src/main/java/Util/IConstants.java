package Util;

/**
 * The interface hold constants.
 * @author ashanw
 */
public interface IConstants {
    public static final String TWITTER_PROPERTIES = "twitter.properties";
    public static final String KAFKA_PROPERTIES = "kafka.properties";
    public static final String ELASTICSEARCH_PROPERTIES = "elasticsearch.properties";
    public static final String JOB_NAME = "twitter-streaming";

    //Contains the elasticsearch constants.
    interface ElasticSearch {
        public static final String CREATED_AT = "created_at";
        public static final String SENTIMENT_SCORE = "sentiment_score";
        public static final String TWEET = "tweet";
        public static final String LANGUAGE = "language";
        public static final String LOCATION = "location";
    }
}