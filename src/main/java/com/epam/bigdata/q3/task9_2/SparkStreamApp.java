package com.epam.bigdata.q3.task9_2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.spark.streaming.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.spark_project.guava.collect.HashBasedTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

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

	        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingApp")
	        		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

	        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

	        Map<String, Integer> topicMap = new HashMap<>();
	        for (String topic : topics) {
	            topicMap.put(topic, numThreads);
	        }

	        JavaPairReceiverInputDStream<String, String> messages =
	                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);
	        
	        
	        /*

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
	        
	        
	        //--------------------------------------------------------------------------------------------
	     // create connection with HBase
	        Configuration config = null;
	        try {
	               config = HBaseConfiguration.create();
	               config.set("hbase.zookeeper.quorum", "127.0.0.1");
	               //config.set("hbase.zookeeper.property.clientPort","2181");
	               //config.set("hbase.master", "127.0.0.1:60000");
	               HBaseAdmin.checkHBaseAvailable(config);
	               System.out.println("HBase is running!");
	             } 
	        catch (MasterNotRunningException e) {
	                    System.out.println("HBase is not running!");
	                    System.exit(1);
	        }catch (Exception ce){ 
	                ce.printStackTrace();
	        }
	        
	        config.set(TableInputFormat.INPUT_TABLE, table);
	        
	     // new Hadoop API configuration
	        Job newAPIJobConfiguration1 = Job.getInstance(config);
	        newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
	        newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
	       
	     // create Key, Value pair to store in HBase
	        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = words.mapToPair(
	            new PairFunction<Row, ImmutableBytesWritable, Put>() {
	          @Override
	          public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
	               
	             Put put = new Put(Bytes.toBytes(row.getString(0)));
	             put.add(Bytes.toBytes("columFamily"), Bytes.toBytes("columnQualifier1"), Bytes.toBytes(row.getString(1)));
	             put.add(Bytes.toBytes("columFamily"), Bytes.toBytes("columnQualifier2"), Bytes.toBytes(row.getString(2)));
	           
	                 return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);     
	          }
	           });
	           
	        
	        wordCounts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());

	        /*
	       wordCounts.foreachRDD((values, count) -> {
	    	   values.foreach(new VoidFunction<Tuple2<String, Integer>> () {

				@Override
				public void call(Tuple2<String, Integer> tuple) throws Exception {
				
					
				}});
	       });
	 
	 */
	 
	 
	 
	 

/*
 * wordCounts.foreach(new Function2<JavaPairRDD<String,Integer>, Time, Void>() {

			@Override
			public Void call(JavaPairRDD<String, Integer> values,
					Time time) throws Exception {
				
				values.foreach(new VoidFunction<Tuple2<String, Integer>> () {

					@Override
					public void call(Tuple2<String, Integer> tuple)
							throws Exception {
						HBaseCounterIncrementor incrementor = 
								HBaseCounterIncrementor.getInstance(broadcastTableName.value(), broadcastColumnFamily.value());
						incrementor.increment("Counter", tuple._1(), tuple._2());
						System.out.println("------------------------------- Counter:" + tuple._1() + "," + tuple._2());
						
					}} );
				
				return null;
			}});
 */
	        
	//----------------------------------------------------------------------------

	        // create connection with HBase
	        Configuration config = null;
	        try {
	               config = HBaseConfiguration.create();
	               config.set("hbase.zookeeper.quorum", "127.0.0.1");
	               config.set("hbase.zookeeper.property.clientPort","2181");
	               config.set("hbase.master", "127.0.0.1:60000");
	               config.set("zookeeper.znode.parent", "/hbase-unsecure");
	               HBaseAdmin.checkHBaseAvailable(config);
	               System.out.println("HBase is running!");
	             } 
	        catch (MasterNotRunningException e) {
	                    System.out.println("HBase is not running!");
	                    System.exit(1);
	        }catch (Exception ce){ 
	                ce.printStackTrace();
	        }
	        
	        config.set(TableInputFormat.INPUT_TABLE, table);
	        
	     // new Hadoop API configuration
	        Job newAPIJobConfiguration1 = Job.getInstance(config);
	        newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
	        newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
	        
	        
	        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
	            @Override
	            public String call(Tuple2<String, String> tuple2) {

	                System.out.println("#1 " + tuple2.toString());
	                return tuple2._2();
	            }
	        });
	        
	        
	        JavaPairDStream<ImmutableBytesWritable, Put> hbasePuts= lines.mapToPair(
                    new PairFunction<String, ImmutableBytesWritable, Put>(){

                @Override
                public Tuple2<ImmutableBytesWritable, Put> call(String line) {
                    Put put = new Put(Bytes.toBytes("Rowkey" + Math.random()));
                    put.add(Bytes.toBytes("firstFamily"), Bytes.toBytes("firstColumn"), Bytes.toBytes(line + "fc"));
                    return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
                 }
                });

/*
	        hbasePuts.foreachRDD(new Function<JavaPairRDD<ImmutableBytesWritable, Put>, Void>() {
                @Override
                public Void call(JavaPairRDD<ImmutableBytesWritable, Put> hbasePutJavaPairRDD) throws Exception {
                    hbasePutJavaPairRDD.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
                    //hbasePutJavaPairRDD.saveAsNewAPIHadoopDataset(conf);
                    return null;
                }
            });
            
            */
	        
	        hbasePuts.foreachRDD(hbasePutJavaPairRDD -> {
                    hbasePutJavaPairRDD.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());  
            });
	        
	        

	      //  wordCounts.print();
	        jssc.start();
	        jssc.awaitTermination();
	   
	    }
	     
}
