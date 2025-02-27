package com.example.service;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.*;
import com.databricks.sdk.support.Wait;

import java.util.List;
import java.util.concurrent.TimeoutException;

public class JobService {
    private final JobsAPI jobsAPI;

    public JobService(final WorkspaceClient client) {
        this.jobsAPI = client.jobs();
    }

    public Long createJob(final String clusterId, final Task task) {
        final JobCluster jobCluster = new JobCluster();
        jobCluster.setJobClusterKey(clusterId);

        final CreateJob job = new CreateJob();
        job.setJobClusters(List.of(jobCluster));
        job.setTasks(List.of(task));

        CreateResponse response = this.jobsAPI.create(job);

        return response.getJobId();
    }

    public void runJob(final Long jobId) throws TimeoutException {
        Wait<Run, RunNowResponse> response = this.jobsAPI.runNow(jobId);

        response.get();
    }
}
