package util;

/**
 * The interface hold constants.
 * @author ashanw
 */
public interface IConstants {
    String TWITTER_PROPERTIES = "twitter.properties";
    String KAFKA_PROPERTIES = "kafka.properties";
    String ES_PROPERTIES = "elasticsearch.properties";
    String JOB_NAME = "twitter-streaming";
    String NOT_AVAILABLE = "N/A";

    interface Es {
        String CREATED_AT = "created_at";
        String SENTIMENT = "sentiment_score";
        String TWEET = "tweet";
        String LANGUAGE = "language";
        String LOCATION = "location";
    }

    interface Twitter {
        String EXT_TWEET = "extended_tweet";
        String FULL_TEXT = "full_text";
        String TEXT = "text";
        String LANG = "lang";
        String CREATED_AT = "created_at";
        String PLACE = "place";
        String COUNTRY = "country";
        String EN = "en";
    }
}