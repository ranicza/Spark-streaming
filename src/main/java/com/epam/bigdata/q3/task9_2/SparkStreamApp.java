package com.epam.bigdata.q3.task9_2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.streaming.*;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;


import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.util.Bytes;

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
	 // private static final String SPLIT = "\\s+";
	  private static final String SPLIT = "\\t";


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
	        String tableName = args[5];
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
	        JavaDStream<String> lines = messages.map(tuple2 -> {
	            Configuration conf = HBaseConfiguration.create();
	            conf.set("hbase.zookeeper.property.clientPort", "2181");
	            conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
	            conf.set("zookeeper.znode.parent", "/hbase-unsecure");
	            HTable table = new HTable(conf, tableName);
	                Put put = new Put(Bytes.toBytes(new java.util.Date().getTime()));
	                put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("column_info"), Bytes.toBytes(tuple2._2()));
	                try {
	                    table.put(put);
	                    
	                   
	                } catch (IOException e) {
	                    System.out.println("IOException" + e.getMessage());
	                }
	                System.out.println("write to table: " + tuple2.toString());
	              //  table.close();
	                return new String(tuple2._2());
	        });
	        
	      */
	      JavaDStream<String> lines = messages.map(tuple2 -> {
	    	  Configuration conf = getConfig();
	          HTable table = new HTable(conf, tableName);
	          
	    	  // Split each line into fields
	    	  String[] fields = tuple2._2().toString().split(SPLIT);  
//	    	  String rowKey = fields[0]; 	    	      	  
//	    	  Put put = new Put(Bytes.toBytes(rowKey));			
	    	  
	    	  Put put = new Put(Bytes.toBytes(new java.util.Date().getTime()));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bid_Id"), Bytes.toBytes(fields[1]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("timestamp_data"), Bytes.toBytes(fields[2]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ipinyou_Id"), Bytes.toBytes(fields[3]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("user_agent"), Bytes.toBytes(fields[4]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ip"), Bytes.toBytes(fields[5]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("region"), Bytes.toBytes(fields[6]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("city"), Bytes.toBytes(fields[7]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ad_exchange"), Bytes.toBytes(fields[8]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("domain"), Bytes.toBytes(fields[9]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("url"), Bytes.toBytes(fields[10]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("anonymous_url_id"), Bytes.toBytes(fields[11]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ad_slot_id"), Bytes.toBytes(fields[12]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ad_slot_width"), Bytes.toBytes(fields[13]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ad_slot_height"), Bytes.toBytes(fields[14]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ad_slot_visibility"), Bytes.toBytes(fields[15]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ad_slot_format"), Bytes.toBytes(fields[16]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("paying_price"), Bytes.toBytes(fields[17]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("creative_id"), Bytes.toBytes(fields[18]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bidding_price"), Bytes.toBytes(fields[19]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("advertiser_id"), Bytes.toBytes(fields[20]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("user_tags"), Bytes.toBytes(fields[21]));
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stream_id"), Bytes.toBytes(fields[22]));
              try {
                  table.put(put);
                  
                 
              } catch (IOException e) {
                  System.out.println("IOException" + e.getMessage());
              }
              System.out.println("write to table: " + tuple2.toString());
            //  table.close();
              return new String(tuple2._2());

	        });
	      
	      
	        
	     
	     
	        // Split each line into fields
	        JavaDStream<String> fields = messages.flatMap(line -> {
	        	return Arrays.asList(line.toString().split(SPLIT)).iterator();
	        });
	        
	        fields.foreachRDD(line ->
	        	System.out.println(line)
	        );
	        
	        
	        
	        JavaDStream<String> rows = fields.map(field-> {
	            Configuration conf = getConfig();
	            HTable table = new HTable(conf, tableName);
	            Put put = new Put(Bytes.toBytes(new java.util.Date().getTime()));
                put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("column_info"), Bytes.toBytes(field));
	            		

	                try {
	                    table.put(put);
	                    
	                   
	                } catch (IOException e) {
	                    System.out.println("IOException" + e.getMessage());
	                }
	                System.out.println("write to table: " + field.toString());
	              //  table.close();
	                return new String(field);
	        });
	     
	        
	        /*
	        JavaDStream<String> rows = messages.map(tuple2 -> {
	            Configuration conf = getConfig();
	            HTable table = new HTable(conf, tableName);
	                Put put = new Put(Bytes.toBytes(new java.util.Date().getTime()));
	                put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("column_info"), Bytes.toBytes(tuple2._2()));
	                try {
	                    table.put(put);
	                    
	                   
	                } catch (IOException e) {
	                    System.out.println("IOException" + e.getMessage());
	                }
	                System.out.println("write to table: " + tuple2.toString());
	              //  table.close();
	                return new String(tuple2._2());
	        });
	        	      

*/
	        
	        JavaDStream<String> lines1 = messages.map(tuple2 -> {
	            System.out.println("#lines1: " + tuple2.toString());
	            return tuple2._2();
	        });

	 /*
	        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
	        JavaPairDStream<String, Integer> wordCounts = words
	                .mapToPair(s -> new Tuple2<>(s, 1))
	                .reduceByKey((i1,i2) -> i1 + i2);
	                 wordCounts.print();
*/
	       
	        jssc.start();
	        jssc.awaitTermination();
	   
	    }
	     
        private static Configuration getConfig() {
        	Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
            conf.set("zookeeper.znode.parent", "/hbase-unsecure");
            return conf;
        }
}
