package Agent;

import Util.IConstants;
import Util.PropertyFile;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

/**
 * The class in intended to act as a kafka receiver.
 * @author ashanw
 * @since 3-2-2020
 */
public class KafkaAgent {

    public static Logger LOG = Logger.getLogger(KafkaAgent.class);

    /**
     * This class is intended to perform the required processing.
     * The method implements the FlatMapFunction thus returning the processed and transformed response.
     */
    private static class processTweet implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String jsonTweet, Collector<String> out)  {
            try {
                JSONObject jsonObject = (JSONObject) new JSONParser().parse(jsonTweet);
                final String tweet = jsonObject.get(IConstants.ElasticSearch.TWEET).toString();
                final Date createdDate = new Date(jsonObject.get(IConstants.ElasticSearch.CREATED_AT).toString());
                final String language = jsonObject.get(IConstants.ElasticSearch.LANGUAGE).toString();
                final String location = jsonObject.get(IConstants.ElasticSearch.LOCATION) != null ?
                        jsonObject.get(IConstants.ElasticSearch.LOCATION).toString() : null;
                final int sentimentScore = Integer.parseInt(jsonObject.get(IConstants.ElasticSearch.SENTIMENT_SCORE).toString());

                publishToElasticIndex(tweet, createdDate, language, location, sentimentScore); //publish to elasticsearch index
                out.collect(jsonTweet);
            } catch (ParseException | NumberFormatException ex) {
                LOG.error("Exception occurred when parsing or with number format.");
            }
        }
    }

    /**
     * The method is intended to publish to elasticsearch index.
     * @param tweet tweet posted
     * @param createdAt the date and time of the tweet
     * @param language language of which the tweet was published
     * @param location location of the user
     * @param sentimentScore the sentiment value given for the tweet.
     */
    private static void publishToElasticIndex(final String tweet,
                                              final Date createdAt,
                                              final String language,
                                              final String location,
                                              final int sentimentScore) {

        try {
            Properties properties = PropertyFile.getProperties(IConstants.ELASTICSEARCH_PROPERTIES);
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field(IConstants.ElasticSearch.TWEET, tweet);
                builder.field(IConstants.ElasticSearch.CREATED_AT, createdAt);
                builder.field(IConstants.ElasticSearch.LANGUAGE, language);
                builder.field(IConstants.ElasticSearch.LOCATION, location);
                builder.field(IConstants.ElasticSearch.SENTIMENT_SCORE, sentimentScore);
            }
            builder.endObject();
            RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                    new HttpHost(properties.getProperty("host"), Integer.parseInt(properties.getProperty("port")))));
            IndexRequest indexRequest = new IndexRequest(properties.getProperty("index.name"));
            indexRequest.source(builder);
            client.index(indexRequest, RequestOptions.DEFAULT);
            client.close();
        } catch (IOException ex){
            LOG.error("Error occurred when publishing to Elastic Search Index", ex);
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties kafkaProperties = PropertyFile.getProperties(IConstants.KAFKA_PROPERTIES);
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>
                (kafkaProperties.getProperty("topic.name"), new SimpleStringSchema(), kafkaProperties);
        environment.addSource(kafkaSource).flatMap(new processTweet());
        environment.execute(IConstants.JOB_NAME);
    }
}