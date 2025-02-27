package com.example.service;

import com.databricks.sdk.service.jobs.Task;

public class TaskService {
    public Task createTask(final String clusterId) {
        Task task = new Task();
        task.setJobClusterKey(clusterId);

        return task;
    }
}
