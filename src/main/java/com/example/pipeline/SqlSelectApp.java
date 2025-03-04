package com.example.pipeline;

import com.example.config.SparkSessionProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SqlSelectApp {
    public static void main(String[] args) {
        SqlSelectApp.run(SparkSessionProvider.getSparkSession());
    }

    private static void run(final SparkSession sparkSession) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(
                        "geo",
                        DataTypes.StringType,
                        true),
                DataTypes.createStructField(
                        "yr1980",
                        DataTypes.DoubleType,
                        false)});

        Dataset<Row> df = sparkSession.read().format("csv")
                .option("header", true)
                .schema(schema)
                .load("data/population-country.csv");

        df.createOrReplaceTempView("geodata");
        df.printSchema();

        Dataset<Row> smallCountries = sparkSession.sql("SELECT * FROM geodata WHERE yr1980 < 1 ORDER BY 2 LIMIT 5");

        smallCountries.show(10, false);

        sparkSession.stop();
    }

}
