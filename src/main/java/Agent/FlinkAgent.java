package agent;

import dto.PayLoad;
import nlp.SentimentAnalyzer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
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
        List<String> termsList;
        try {
            Properties properties = PropertyFile.getProperties(IConstants.TWITTER_PROPERTIES);
            String terms = properties.getProperty("twitter.terms");
            termsList = Arrays.asList(terms.split("\\s*,\\s*"));
        }catch (IOException ex){
            termsList = new ArrayList<>();
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

    /**
     * The following method is intended to return the twitter payload after the required transformations.
     * @param jsonNode jsonNode
     * @return twitter payload object
     */
    private static PayLoad getPayLoad(final JsonNode jsonNode){
        final boolean isEnglish = jsonNode.has(IConstants.Twitter.LANG) && IConstants.Twitter.EN.equals(jsonNode.get(IConstants.Twitter.LANG).asText());
        final boolean containsExtendedTweet = jsonNode.has(IConstants.Twitter.EXT_TWEET);
        PayLoad payLoad = new PayLoad();

        String tweet;
        if(containsExtendedTweet){
            tweet = jsonNode.get(IConstants.Twitter.EXT_TWEET).get(IConstants.Twitter.FULL_TEXT).textValue();
        } else {
            tweet = jsonNode.get(IConstants.Twitter.TEXT).textValue();
        }
        payLoad.setTweet(tweet);
        payLoad.setLanguage(jsonNode.get(IConstants.Twitter.LANG).textValue());
        payLoad.setCreatedAt(jsonNode.get(IConstants.Twitter.CREATED_AT).textValue());
        payLoad.setSentiment(isEnglish ? SentimentAnalyzer.predictSentiment(tweet) : -1);

        String location;
        if (!jsonNode.get(IConstants.Twitter.PLACE).isEmpty()) {
            location = jsonNode.get(IConstants.Twitter.PLACE).get(IConstants.Twitter.COUNTRY).textValue();
        } else {
            location = IConstants.NOT_AVAILABLE;
        }
        payLoad.setLocation(location);
        return payLoad;
    }

    /**
     * This class is intended to perform the required processing before pushing to elastic-search.
     */
    private static class TweetFlatMapper implements FlatMapFunction<String, PayLoad> {
        @Override
        public void flatMap(String tweet, Collector<PayLoad> out) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode jsonNode = mapper.readValue(tweet, JsonNode.class);
                PayLoad payLoad = getPayLoad(jsonNode);
                out.collect(payLoad);
            } catch (Exception ex) {
                LOG.error("Exception occurred when getting the tweet from twitter String! ", ex.getCause());
            }
        }
    }

    /**
     * The following class is responsible for serializing the twitter payload.
     */
    static class PayLoadSerializationSchema implements KafkaSerializationSchema<PayLoad> {

        private String topic;
        private ObjectMapper mapper;

        public PayLoadSerializationSchema(String topic) {
            super();
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(PayLoad payLoad, @Nullable Long aLong) {
            byte[] b = null;
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                b = mapper.writeValueAsBytes(payLoad);
            } catch (JsonProcessingException ex) {
                LOG.error("Error when serializing kafka schema! ", ex.getCause());
            }
            return new ProducerRecord<>(topic, b);
        }
    }

    /**
     * Agent init
     * @param args no arguments taken
     */
    public static void main(String[] args) {
        try {
            Properties twitterProperties = PropertyFile.getProperties(IConstants.TWITTER_PROPERTIES);
            TwitterSource twitterSource = new TwitterSource(twitterProperties);
            twitterSource.setCustomEndpointInitializer(new TweetFilter());
            StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<PayLoad> streamSource = environment.addSource(twitterSource).flatMap(new TweetFlatMapper());

            //Configure kafka sink.
            Properties kafkaProperties = PropertyFile.getProperties(IConstants.KAFKA_PROPERTIES);
            String producerTopic = kafkaProperties.getProperty("topic.name");
            streamSource.addSink(new FlinkKafkaProducer(producerTopic, new PayLoadSerializationSchema(producerTopic),
                    kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
            streamSource.print();
            environment.execute(IConstants.JOB_NAME);
        } catch (Exception ex) {
            LOG.error("Exception occurred executing the environment ", ex.getCause());
        }
    }
}