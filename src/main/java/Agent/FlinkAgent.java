package Agent;

import Nlp.SentimentAnalyzer;
import Util.IConstants;
import Util.PropertyFile;
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
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * The class acts as a Twitter Producer.
 * @author ashanw
 * @since 2-29-2020
 */
public class FlinkAgent {

    public static Logger LOG = Logger.getLogger(FlinkAgent.class);

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
        final boolean isEnglish = jsonNode.has(IConstants.Twitter.LANG) &&
                                  jsonNode.get(IConstants.Twitter.LANG).asText().equals(IConstants.Twitter.EN);
        final boolean containsExtendedTweet = jsonNode.has(IConstants.Twitter.EXTENDED_TWEET);
        String location;
        JSONObject jsonObject = new JSONObject();
        String tweet = containsExtendedTweet ? jsonNode.get(IConstants.Twitter.EXTENDED_TWEET)
                                                       .get(IConstants.Twitter.FULL_TEXT).textValue()
                                             : jsonNode.get(IConstants.Twitter.TEXT).textValue();
        jsonObject.put(IConstants.ElasticSearch.TWEET, tweet);
        jsonObject.put(IConstants.ElasticSearch.LANGUAGE, jsonNode.get(IConstants.Twitter.LANG).textValue());
        jsonObject.put(IConstants.ElasticSearch.CREATED_AT, jsonNode.get(IConstants.Twitter.CREATED_AT).textValue());
        jsonObject.put(IConstants.ElasticSearch.SENTIMENT_SCORE, isEnglish ? SentimentAnalyzer.predictSentiment(tweet) : -1);

        if (!jsonNode.get(IConstants.Twitter.PLACE).isEmpty()) {
            location = jsonNode.get(IConstants.Twitter.PLACE).get(IConstants.Twitter.COUNTRY).textValue();
        } else {
            location = IConstants.NOT_AVAILABLE;
        }
        jsonObject.put(IConstants.ElasticSearch.LOCATION, location);
        return jsonObject.toJSONString();
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