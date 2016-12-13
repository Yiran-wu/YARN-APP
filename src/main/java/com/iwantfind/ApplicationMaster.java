package com.iwantfind;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by YiRan on 12/13/16.
 */
public class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationMaster.class);


    /* Parameters sent from Client. */
    private final int mWorkerCpu;
    private final int mWorkerMemInMB;
    private final int mNumWorkers;


    private final String mResourcePath;
    private final int mMaxWorkersPerHost = 1;

    private static final NodeState[] USABLE_NODE_STATES;

    private final YarnConfiguration mYarnConf = new YarnConfiguration();

    private final Multiset<String> mWorkerHosts;
    /**
     * The count starts at 1, then becomes 0 when the application is done.
     */
    private final CountDownLatch mApplicationDoneLatch;

    /**
     * Client to talk to Resource Manager.
     */
    private final AMRMClientAsync<ContainerRequest> mRMClient;
    /**
     * Client to talk to Node Manager.
     */
    private final NMClient mNMClient;
    /**
     * Client Resource Manager Service.
     */
    private final YarnClient mYarnClient;
    /**
     * Network address of the container allocated for Alluxio master.
     */
    private String mMasterContainerNetAddress;
    private CountDownLatch mOutstandingWorkerContainerRequestsLatch = null;


    static {
        List<NodeState> usableStates = Lists.newArrayList();
        for (NodeState nodeState : NodeState.values()) {
            if (!nodeState.isUnusable()) {
                usableStates.add(nodeState);
            }
        }
        USABLE_NODE_STATES = usableStates.toArray(new NodeState[usableStates.size()]);
    }

    public interface AMRMClientAsyncFactory {
        /**
         * @param heartbeatMs the interval at which to send heartbeats to the resource manager
         * @param handler     a handler for callbacks from the resource manager
         * @return a client for making requests to the resource manager
         */
        AMRMClientAsync<ContainerRequest> createAMRMClientAsync(int heartbeatMs,
                                                                AMRMClientAsync.CallbackHandler handler);
    }
    //private volatile ContainerAllocator mContainerAllocator;

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            // Releasing worker containers because we already have workers on their host will generate a
            // callback to this method, so we use info instead of error.
            if (status.getExitStatus() == ContainerExitStatus.ABORTED) {
                LOG.info("Aborted container {}", status.getContainerId());
            } else {
                LOG.error("Container {} completed with exit status {}", status.getContainerId(),
                        status.getExitStatus());
            }
        }
    }

    public void onContainersAllocated(List<Container> containers) {
        int count = 0;
        for (Container container : containers) {
            System.out.println("Allocated containers[" + count++ + "] " + container.toString());
        }
        launchWorkerContainers(containers);
    }

    public void onShutdownRequest() {
        mApplicationDoneLatch.countDown();
    }

    public void onNodesUpdated(List<NodeReport> list) {

    }

    public float getProgress() {
        return 0;
    }

    public void onError(Throwable throwable) {

    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("num_workers", true, "Number of Alluxio workers to launch. Default 1");
        LOG.info("Starting Application Master with args {}", Arrays.toString(args));
        final CommandLine cliParser = new GnuParser().parse(options, args);
        runApplicationMaster(cliParser);
    }

    /**
     * Run the application master.
     */
    private static void runApplicationMaster(final CommandLine cliParser) throws Exception {
        int numWorkers = Integer.parseInt(cliParser.getOptionValue("num_workers", "1"));
        String masterAddress = cliParser.getOptionValue("master_address");


        ApplicationMaster applicationMaster =
                new ApplicationMaster(numWorkers, masterAddress, "path");
        applicationMaster.start();
        applicationMaster.requestContainers();
        applicationMaster.waitForShutdown();
        applicationMaster.stop();
    }

    public ApplicationMaster(int numWorkers, String masterAddress, String resourcePath) {
        this(numWorkers, masterAddress, resourcePath, YarnClient.createYarnClient(),
                NMClient.createNMClient(), new AMRMClientAsyncFactory() {
                    @Override
                    public AMRMClientAsync<ContainerRequest> createAMRMClientAsync(int heartbeatMs,
                                                                                   AMRMClientAsync.CallbackHandler handler) {
                        return AMRMClientAsync.createAMRMClientAsync(heartbeatMs, handler);
                    }
                });
    }

    public ApplicationMaster(int numWorkers, String masterAddress, String resourcePath,
                             YarnClient yarnClient, NMClient nMClient, AMRMClientAsyncFactory amrmFactory) {

        mWorkerCpu = 1;
        // memory for running worker
        mWorkerMemInMB = 1024;
        mNumWorkers = numWorkers;

        mResourcePath = resourcePath;
        mWorkerHosts = ConcurrentHashMultiset.create();
        mApplicationDoneLatch = new CountDownLatch(1);
        mYarnClient = yarnClient;
        mNMClient = nMClient;

        // Heartbeat to the resource manager every 500ms.
        mRMClient = AMRMClientAsync.createAMRMClientAsync(500, this);
    }

    /**
     * Starts the application master.
     *
     * @throws IOException   if registering the application master fails due to an IO error
     * @throws YarnException if registering the application master fails due to an internal Yarn error
     */
    public void start() throws YarnException, IOException {

        mNMClient.init(mYarnConf);
        mNMClient.start();

        mRMClient.init(mYarnConf);
        mRMClient.start();

        mYarnClient.init(mYarnConf);
        mYarnClient.start();

        // Register with ResourceManager
        String hostname = InetAddress.getLocalHost().toString();
        mRMClient.registerApplicationMaster(hostname, 0 /* port */, "" /* tracking url */);
        LOG.info("ApplicationMaster {}  registered,", hostname);
    }

    public void requestContainers() throws Exception {


        int round = 0;
        while (mWorkerHosts.size() < mNumWorkers) {
            requestWorkerContainers();
            LOG.info("Waiting for {} worker containers to be allocated",
                    mOutstandingWorkerContainerRequestsLatch.getCount());
            // TODO(andrew): Handle the case where something goes wrong and some worker containers never
            // get allocated. See ALLUXIO-1410
            mOutstandingWorkerContainerRequestsLatch.await();
            round++;
        }
        if (mWorkerHosts.size() < mNumWorkers) {
            LOG.error(
                    "Could not request {} workers from yarn resource manager after {} tries. "
                            + "Proceeding with {} workers",
                    mNumWorkers, 10, mWorkerHosts.size());
        }

        LOG.info("Master and workers are launched");

    }

    public void waitForShutdown() throws InterruptedException {
        mApplicationDoneLatch.await();
    }

    private void launchWorkerContainers(List<Container> containers) {
        final String command = YarnUtils.buildCommand(YarnUtils.YarnContainerType.WORKER);

        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        ctx.setCommands(Lists.newArrayList(command));
        ctx.setLocalResources(setupLocalResources(mResourcePath));
//        ctx.setEnvironment(setupWorkerEnvironment(mMasterContainerNetAddress, mRamdiskMemInMB));
        for (Container container : containers) {
            synchronized (mWorkerHosts) {
                LOG.info("===========" + ctx.getCommands());
                if (mWorkerHosts.size() >= mNumWorkers
                        || mWorkerHosts.count(container.getNodeId().getHost()) >= mMaxWorkersPerHost) {
                    // 1. Yarn will sometimes offer more containers than were requested, so we ignore offers
                    // when mWorkerHosts.size() >= mNumWorkers
                    // 2. Avoid re-using nodes if they already have the maximum number of workers
                    LOG.info("Releasing assigned container on {}", container.getNodeId().getHost());
                    mRMClient.releaseAssignedContainer(container.getId());
                } else {
                    try {
                        LOG.info("Launching container {} for worker {} on {} with worker command: {} , ContainerInfo:{} , ctx:{}",
                                container.getId(), mWorkerHosts.size(), container.getNodeHttpAddress(), command, container.toString(), ctx);
                        mNMClient.startContainer(container, ctx);
                        mWorkerHosts.add(container.getNodeId().getHost());
                    } catch (Exception e) {
                        LOG.error("Error launching container {}", container.getId(), e);
                    }
                }
                mOutstandingWorkerContainerRequestsLatch.countDown();
            }
        }
    }

    /**
     * Requests containers for the workers, attempting to get containers on separate nodes.
     */
    private void requestWorkerContainers() throws Exception {
        LOG.info("Requesting worker containers");
        // Resource requirements for worker containers
        Resource workerResource = Records.newRecord(Resource.class);
        workerResource.setMemory(mWorkerMemInMB);
        workerResource.setVirtualCores(mWorkerCpu);
        int currentNumWorkers = mWorkerHosts.size();
        int neededWorkers = mNumWorkers - currentNumWorkers;

        mOutstandingWorkerContainerRequestsLatch = new CountDownLatch(neededWorkers);
        String[] hosts;
        boolean relaxLocality = false;
        hosts = getUnfilledWorkerHosts();
        if (hosts.length < neededWorkers) {
            throw new RuntimeException(
                    "do not have  enough nodes, need " + neededWorkers + ", have " + hosts.length);
        }
        // Make container requests for workers to ResourceManager
        for (int i = currentNumWorkers; i < mNumWorkers; i++) {
            // TODO(andrew): Consider partitioning the available hosts among the worker requests
            ContainerRequest containerAsk = new ContainerRequest(workerResource, hosts,
                    null /* any racks */, Priority.newInstance(0), relaxLocality);
            LOG.info("Making resource request for Alluxio worker {}: cpu {} memory {} MB on hosts {}", i,
                    workerResource.getVirtualCores(), workerResource.getMemory(), hosts);
            mRMClient.addContainerRequest(containerAsk);
        }
    }

    private String[] getUnfilledWorkerHosts() throws Exception {
        List<String> unusedHosts = Lists.newArrayList();
        for (String host : getNodeHosts(mYarnClient)) {
            if (mWorkerHosts.count(host) < mMaxWorkersPerHost) {
                unusedHosts.add(host);
            }
        }
        return unusedHosts.toArray(new String[]{});
    }

    private static Set<String> getNodeHosts(YarnClient yarnClient) throws YarnException, IOException {
        ImmutableSet.Builder<String> nodeHosts = ImmutableSet.builder();
        for (NodeReport runningNode : yarnClient.getNodeReports(USABLE_NODE_STATES)) {
            nodeHosts.add(runningNode.getNodeId().getHost());
        }
        return nodeHosts.build();
    }

    public void stop() {
        try {
            mRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        } catch (YarnException e) {
            LOG.error("Failed to unregister application", e);
        } catch (IOException e) {
            LOG.error("Failed to unregister application", e);
        }
        mRMClient.stop();
        mYarnClient.stop();
    }

    private static Map<String, LocalResource> setupLocalResources(String resourcePath) {
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        return localResources;

    }

    private static Map<String, String> setupWorkerEnvironment(String masterContainerNetAddress,
                                                              int ramdiskMemInMB) {
        Map<String, String> env = new HashMap<String, String>();

        return env;
    }
}
