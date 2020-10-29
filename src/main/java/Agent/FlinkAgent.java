package agent;

import nlp.SentimentAnalyzer;
import util.IConstants;
import util.PropertyFile;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.connectors.twitter.TwitterSource.EndpointInitializer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * The class acts as a Twitter Producer.
 * @author ashanw
 * @since 2-29-2020
 */
public class FlinkAgent {

    private static Logger LOG = Logger.getLogger(FlinkAgent.class);

    /**
     * The following method is intended to return the twitter terms.
     * @return twitter terms
     */
    private static List<String> getTwitterTerms() {
        List termsList;
        try {
            Properties properties = PropertyFile.getProperties(IConstants.TWITTER_PROPERTIES);
            String terms = properties.getProperty("twitter.terms");
            termsList = Arrays.asList(terms.split("\\s*,\\s*"));
        }catch (IOException ex){
            termsList = Collections.EMPTY_LIST;
        }
        return termsList;
    }

    /**
     * This class is intended to initialize the endpoint and the terms to track.
     */
    private static class TweetFilter implements EndpointInitializer, Serializable  {
        @Override
        public StreamingEndpoint createEndpoint() {
            StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
            endpoint.trackTerms(getTwitterTerms());
            return endpoint;
        }
    }

    private static String getJsonString(final JsonNode jsonNode){
        final boolean isEnglish = jsonNode.has(IConstants.Twitter.LANG) && IConstants.Twitter.EN.equals(jsonNode.get(IConstants.Twitter.LANG).asText());
        final boolean containsExtendedTweet = jsonNode.has(IConstants.Twitter.EXT_TWEET);
        Map<String, Object> twitterMap = new LinkedHashMap<>();

        String tweet;
        if(containsExtendedTweet){
            tweet = jsonNode.get(IConstants.Twitter.EXT_TWEET).get(IConstants.Twitter.FULL_TEXT).textValue();
        } else {
            tweet = jsonNode.get(IConstants.Twitter.TEXT).textValue();
        }
        twitterMap.put(IConstants.Es.TWEET, tweet);
        twitterMap.put(IConstants.Es.LANGUAGE, jsonNode.get(IConstants.Twitter.LANG).textValue());
        twitterMap.put(IConstants.Es.CREATED_AT, jsonNode.get(IConstants.Twitter.CREATED_AT).textValue());
        twitterMap.put(IConstants.Es.SENTIMENT, isEnglish ? SentimentAnalyzer.predictSentiment(tweet) : -1);

        String location;
        if (!jsonNode.get(IConstants.Twitter.PLACE).isEmpty()) {
            location = jsonNode.get(IConstants.Twitter.PLACE).get(IConstants.Twitter.COUNTRY).textValue();
        } else {
            location = IConstants.NOT_AVAILABLE;
        }
        twitterMap.put(IConstants.Es.LOCATION, location);
        return twitterMap.toString();
    }

    /**
     * This class is intended to perform the required processing before pushing to elastic-search.
     */
    private static class TweetFlatMapper implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String tweet, Collector<String> out) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode jsonNode = mapper.readValue(tweet, JsonNode.class);
                String jsonString = getJsonString(jsonNode);
                if(null != jsonString) {
                    out.collect(jsonString);
                }
            } catch (Exception ex) {
                LOG.error("Exception occurred when getting the tweet from twitter String! ", ex.getCause());
            }
        }
    }

    public static void main(String[] args) {
        try {
            Properties twitterProperties = PropertyFile.getProperties(IConstants.TWITTER_PROPERTIES);
            TwitterSource twitterSource = new TwitterSource(twitterProperties);
            twitterSource.setCustomEndpointInitializer(new TweetFilter());
            StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<String> streamSource = environment.addSource(twitterSource).flatMap(new TweetFlatMapper());

            //Configure kafka sink.
            Properties kafkaProperties = PropertyFile.getProperties(IConstants.KAFKA_PROPERTIES);
            FlinkKafkaProducer<String> kafkaSource = new FlinkKafkaProducer<>(kafkaProperties.getProperty("topic.name"),
                                                     new SimpleStringSchema(), kafkaProperties);
            streamSource.addSink(kafkaSource);
            environment.execute(IConstants.JOB_NAME);
        } catch (Exception ex) {
            LOG.error("Exception occurred executing the environment ", ex.getCause());
        }
    }
}