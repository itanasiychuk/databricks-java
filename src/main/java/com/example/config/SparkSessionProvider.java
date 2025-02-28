package com.example.config;

import org.apache.spark.sql.SparkSession;

public class SparkSessionProvider {
    public static SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("Java Spark ETL Pipeline")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();
    }
}
