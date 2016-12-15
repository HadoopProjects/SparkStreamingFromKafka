
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

@SuppressWarnings("unchecked")
public class SparkStreamingKafka {

    @SuppressWarnings("serial")
    public static void main(String[] str) throws InterruptedException {

        String brokers = "localhost:9091";
        String topics = "StreamingTopic";

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setMaster("local")
                .setAppName("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
                Durations.seconds(5));

        Set<String> topicsSet = new HashSet<String>(Arrays.asList(topics
                .split(",")));
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils
                .createDirectStream(jssc, String.class, String.class,
                        StringDecoder.class, StringDecoder.class, kafkaParams,
                        topicsSet);


        JavaDStream<String> lines = messages
                .map(new Function<Tuple2<String, String>, String>() {
                    public String call(Tuple2<String, String> tuple2) {
                        return tuple2._2();
                    }
                });

        JavaDStream<String> words = lines
                .flatMap(new FlatMapFunction<String, String>() {

                    public Iterable<String> call(String x) {
                        return Arrays.asList(x.split(" "));
                    }
                });
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();

    }

}
