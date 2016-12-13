package com.iwantfind;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.util.List;

/**
 * Created by YiRan on 12/13/16.
 */
public class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
    public void onContainersCompleted(List<ContainerStatus> list) {

    }

    public void onContainersAllocated(List<Container> list) {

    }

    public void onShutdownRequest() {

    }

    public void onNodesUpdated(List<NodeReport> list) {

    }

    public float getProgress() {
        return 0;
    }

    public void onError(Throwable throwable) {

    }
}
