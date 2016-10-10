package com.epam.bigdata.q3.task9_2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.streaming.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * 
 * @author Maryna_Maroz
 * 
 *  Usage: SparkStreamingApp <master> <zkQuorum> <group> <topics> <numThreads> <table> <columnFamily>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 */
public class SparkStreamApp {
	  private static final Pattern SPACE = Pattern.compile(" ");


	    public static void main(String[] args) throws Exception {
	        if (args.length == 0) {
	            System.err
	                    .println("Usage: SparkStreamingApp {master} {zkQuorum} {group} {topic} {numThreads} {table} {columnFamily}");
	            System.exit(1);
	        }

	        String master = args[0];
	        String zkQuorum = args[1];
	        String group = args[2];
	        String[] topics = args[3].split(",");
	        int numThreads = Integer.parseInt(args[4]);
	        String table = args[5];
	        String columnFamily = args[6];

	        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingApp");

	        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

	        Map<String, Integer> topicMap = new HashMap<>();
	        for (String topic : topics) {
	            topicMap.put(topic, numThreads);
	        }

	        JavaPairReceiverInputDStream<String, String> messages =
	                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

	        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
	            @Override
	            public String call(Tuple2<String, String> tuple2) {

	                System.out.println("#1 " + tuple2.toString());
	                return tuple2._2();
	            }
	        });

	        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	            @Override
	            public Iterator<String> call(String x) {
	                return Arrays.asList(SPACE.split(x)).iterator();
	            }
	        });

	        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
	                new PairFunction<String, String, Integer>() {
	                    @Override
	                    public Tuple2<String, Integer> call(String s) {
	                        return new Tuple2<>(s, 1);
	                    }
	                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
	            @Override
	            public Integer call(Integer i1, Integer i2) {
	                return i1 + i2;
	            }
	        });
	      //  wordCounts.saveAsNewAPIHadoopFiles(prefix, suffix);

	        wordCounts.print();
	        jssc.start();
	        jssc.awaitTermination();
	   
	    }
	     
}
