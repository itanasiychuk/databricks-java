package com.example.pipeline;

import org.apache.hadoop.shaded.org.xbill.DNS.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class CsvToDataSetApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to Dataset")
                .master("local")
                .getOrCreate();

        String filename = "data/tuple-data-file.csv";

        Dataset<Row> df = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "false")
                .load(filename);
        df.show();

        spark.stop();
    }
}
