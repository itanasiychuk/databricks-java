package com.example.pipeline;

import com.databricks.dbutils_v1.NotUsableInsideSparkException;
import com.example.config.SparkSessionProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.databricks.dbutils_v1.DBUtilsHolder.dbutils;

public class DatabricksNotebookApp {
    public static void main(String[] args) throws NotUsableInsideSparkException {
        DatabricksNotebookApp.run(SparkSessionProvider.getSparkSession());
    }

    private static void run(final SparkSession sparkSession) throws NotUsableInsideSparkException {
        dbutils().widgets().text("input", "default", "Input");

        Dataset<Row> df = sparkSession.read().format("csv")
                .option("header", "true")
                .load("/path/to/csv");

        Dataset<Row> transformedDF = df.select("column1", "column2");

        transformedDF.show();

        dbutils().notebook().exit("Notebook execution completed");

        sparkSession.stop();
    }
}



