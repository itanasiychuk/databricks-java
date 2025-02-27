package com.example;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.Task;
import com.example.service.ClusterService;
import com.example.service.DataFrameService;
import com.example.service.JobService;
import com.example.service.TaskService;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class DatabricksApp {
    private static final String CLUSTER_NAME = UUID.randomUUID().toString();
    private static final String PATH = "dbfs:/databricks-datasets/file.txt";
    private static final String DELTA_PATH = "dbfs:/delta/transformed";

    public static void main(String[] args) throws TimeoutException {
        final WorkspaceClient client = new WorkspaceClient();

        final ClusterService clustersService = new ClusterService(client);
        final String clusterId = clustersService.createCluster(CLUSTER_NAME);

        final TaskService taskService = new TaskService();
        final Task task = taskService.createTask(clusterId);

        final JobService jobsService = new JobService(client);
        final Long jobId = jobsService.createJob(clusterId, task);

        jobsService.runJob(jobId);

        final SparkSession session = SparkSession.builder()
                .appName("Databricks DataFrame")
                .getOrCreate();

        final Column condition = new Column("");

        final DataFrameService dataFrameService = new DataFrameService();

        dataFrameService.write(session, condition, PATH, DELTA_PATH);

        final List<Row> readData = dataFrameService.read(session, PATH);

        final List<Row> transformData = dataFrameService.transform(session, condition, PATH);
    }

}
