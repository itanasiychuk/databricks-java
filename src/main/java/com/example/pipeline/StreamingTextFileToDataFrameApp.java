package com.example.pipeline;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingTextFileToDataFrameApp {
    public static void main(String[] args) {
        StreamingTextFileToDataFrameApp.run();
    }

    private static void run() {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
                "Streaming Ingestion File System Text File to Dataframe");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaDStream<String> msgDataStream = jssc.textFileStream("data");

        msgDataStream.print();

        msgDataStream.foreachRDD((VoidFunction<JavaRDD<String>>) rdd -> {
            JavaRDD<Row> rowRDD = rdd.map((Function<String, Row>) msg -> {
                Row row = RowFactory.create(msg);
                return row;
            });

            StructType schema = DataTypes.createStructType(
                    new StructField[]{DataTypes.createStructField("Message", DataTypes.StringType, true)});

            SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
            Dataset<Row> msgDataFrame = spark.createDataFrame(rowRDD, schema);
            msgDataFrame.show();
        });

        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }


    private static class JavaSparkSessionSingleton {
        private static transient SparkSession instance = null;

        public static SparkSession getInstance(SparkConf sparkConf) {
            if (instance == null) {
                instance = SparkSession.builder().config(sparkConf).getOrCreate();
            }
            return instance;
        }
    }
}
