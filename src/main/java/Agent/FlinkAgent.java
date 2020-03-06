package Agent;

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
import java.util.List;
import java.util.Properties;

/**
 * The class acts as a Twitter Producer.
 * @author ashanw
 * @since 2-29-2020
 */
public class FlinkAgent {

    public static Logger LOG = Logger.getLogger(FlinkAgent.class);

    //Stores the twitter search terms.
    private static List<String> twitterTerms;

    //The block fetches the twitter terms from the twitter properties file.
    static {
        try {
            twitterTerms = Arrays.asList(PropertyFile.getTwitterProperties().getProperty("twitter.terms").split("\\s*,\\s*"));
        } catch (IOException ex) {
            LOG.error("Error occurred when reading the twitter terms from twitter.properties file.", ex);
            System.exit(1);
        }
    }

    /**
     * Configure Twitter source and initialize data-stream.
     * @param args arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Properties twitterProperties = PropertyFile.getTwitterProperties();
        TwitterSource twitterSource = new TwitterSource(twitterProperties);
        twitterSource.setCustomEndpointInitializer(new TweetFilter());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> streamSource = environment.addSource(twitterSource).flatMap(new TweetFlatMapper());

        {
            Properties kafkaProperties = PropertyFile.getKafkaProperties();
            FlinkKafkaProducer<String> kafkaSource = new FlinkKafkaProducer<>(kafkaProperties.getProperty("topic.name"),
                    new SimpleStringSchema(), kafkaProperties);
            streamSource.addSink(kafkaSource);
        }
        environment.execute(IConstants.JOB_NAME);
    }

    /**
     * This class is intended to initialize the endpoint and the terms to track.
     */
    private static class TweetFilter implements EndpointInitializer, Serializable {

        @Override
        public StreamingEndpoint createEndpoint() {
            StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
            endpoint.trackTerms(twitterTerms);
            return endpoint;
        }
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
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(IConstants.ElasticSearch.TWEET, jsonNode.get("text").textValue());
                jsonObject.put(IConstants.ElasticSearch.LANGUAGE, jsonNode.get("lang").textValue());
                jsonObject.put(IConstants.ElasticSearch.CREATED_AT, jsonNode.get("created_at").textValue());

                //Add the tweeting country.
                {
                    String location = "N/A";
                    if (!jsonNode.get("place").isEmpty()) {
                        location = jsonNode.get("place").get("country").textValue();
                    }
                    jsonObject.put(IConstants.ElasticSearch.LOCATION, location);
                }

                out.collect(jsonObject.toJSONString());
            } catch (Exception ex) {
                LOG.error("Exception occurred when getting the tweet from twitter String! ", ex.getCause());
            }
        }
    }
}