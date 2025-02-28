package com.example.pipeline;

import com.example.config.SparkSessionProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvToDataSetApp {

    public static void main(String[] args) {
        CsvToDataSetApp.run(SparkSessionProvider.getSparkSession());
    }

    private static void run(final SparkSession sparkSession) {
        final String filename = "data/tuple-data-file.csv";

        final Dataset<Row> df = sparkSession.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "false")
                .load(filename);
        df.show();

        sparkSession.stop();
    }
}
