package com.example.service;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class DataFrameService {

    public void write(final SparkSession session, final Column condition, final String path, final String deltaPath) {
        final Dataset<Row> value = session.read().text(path);

        Dataset<Row> transformed = value.filter(condition);

        transformed.write().format("delta").mode("overwrite").save(deltaPath);
    }

    public List<Row> read(final SparkSession session, final String path) {
        final Dataset<Row> value = session.read().text(path);

        return value.collectAsList();
    }

    public List<Row> transform(final SparkSession session, final Column condition, final String path) {
        final Dataset<Row> value = session.read().text(path);

        return value.filter(condition).collectAsList();
    }

}
