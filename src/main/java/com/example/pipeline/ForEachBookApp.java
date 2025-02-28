package com.example.pipeline;

import com.example.config.SparkSessionProvider;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ForEachBookApp {

    public static void main(String[] args) {
        ForEachBookApp.run(SparkSessionProvider.getSparkSession());
    }

    private static void run(final SparkSession sparkSession) {
        String filename = "data/books.csv";
        Dataset<Row> df = sparkSession.read().format("csv").option("inferSchema", true).option("header", true).load(filename);
        df.show();

        df.foreach(new BookPrinter());

        sparkSession.stop();
    }


    private static final class BookPrinter implements ForeachFunction<Row> {
        @Override
        public void call(Row r) throws Exception {
            System.out.println(r.getString(2) + " can be bought at " + r.getString(4));
        }
    }

}
