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

    //Uses flink kafka connector to subscribe the data-stream.
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties kafkaProperties = PropertyFile.getKafkaProperties();
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>
                (kafkaProperties.getProperty("topic.name"), new SimpleStringSchema(), kafkaProperties);
        environment.addSource(kafkaSource).flatMap(new TweetSentiment()).print();
        environment.execute(IConstants.JOB_NAME);
    }

    //This class is intended to perform the required CEP.
    private static class TweetSentiment implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String tweet, Collector<String> out) {
            publishToElasticIndex(tweet, 1,1, new Date());
            out.collect(tweet);
        }
    }

    //The method is intended to publish to elasticsearch index.
    private static void publishToElasticIndex(String tweet, Integer sentimentMagnitude, Integer sentimentScore, Date date){
        try {
            Properties properties = PropertyFile.getElasticSearchProperties();
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field(IConstants.ElasticSearch.POSTED_DATE, date);
                builder.field(IConstants.ElasticSearch.SENTIMENT_SCORE, sentimentScore);
                builder.field(IConstants.ElasticSearch.SENTIMENT_MAGNITUDE, sentimentMagnitude);
                builder.field(IConstants.ElasticSearch.TWEET, tweet);
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
}