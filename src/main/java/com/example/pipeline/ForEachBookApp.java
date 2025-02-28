package com.example.pipeline;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public class ForEachBookApp implements Serializable {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("For Each Book")
                .master("local[*]")
                .getOrCreate();

        String filename = "data/books.csv";
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load(filename);
        df.show();

        df.foreach(new BookPrinter());

        spark.stop();
    }


    private static final class BookPrinter implements ForeachFunction<Row> {
        @Override
        public void call(Row r) throws Exception {
            System.out.println(
                    r.getString(2) + " can be bought at " + r.getString(4));
        }
    }

}
