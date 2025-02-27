package com.example.service;


import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.mixin.ClustersExt;
import com.databricks.sdk.service.compute.ClusterDetails;
import com.databricks.sdk.service.compute.CreateCluster;
import com.databricks.sdk.service.compute.CreateClusterResponse;
import com.databricks.sdk.service.compute.DeleteCluster;
import com.databricks.sdk.support.Wait;

import java.util.concurrent.TimeoutException;

public class ClusterService {
    private final ClustersExt clusterAPI;

    public ClusterService(final WorkspaceClient client) {
        this.clusterAPI = client.clusters();
    }

    public String createCluster(final String clusterName) throws TimeoutException {
        CreateCluster request = new CreateCluster();
        request.setClusterName(clusterName);

        Wait<ClusterDetails, CreateClusterResponse> createResponse = this.clusterAPI.create(request);
        return createResponse.get().getClusterId();
    }

    public String deleteCluster(final String clusterId) throws TimeoutException {
        DeleteCluster request = new DeleteCluster();
        request.setClusterId(clusterId);

        Wait<ClusterDetails, Void> waitResponse = this.clusterAPI.delete(request);

        return waitResponse.get().getClusterId();
    }
}
