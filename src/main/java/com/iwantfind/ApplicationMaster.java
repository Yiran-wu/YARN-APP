package com.iwantfind;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by YiRan on 12/13/16.
 */
public class ApplicationMaster  extends AbstractService implements AMRMClientAsync.CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationMaster.class);

    private final int mWorkerCpu;      //Worker的VCORE
    private final int mWorkerMemInMB;  //Worker的内存资源
    private final int mNumWorkers;     //启动几个worker?

    private final String mResourcePath;
    private final int mMaxWorkersPerHost = 1;  //每个节点一个worker

    private static final NodeState[] USABLE_NODE_STATES;

    private final YarnConfiguration mYarnConf = new YarnConfiguration();

    private final Multiset<String> mWorkerHosts;          // 记录已经启动worker的节点
    private final CountDownLatch mApplicationDoneLatch;   // 启动计数器， 初始化为1， 0时表示App结束

    private final AMRMClientAsync<ContainerRequest> mRMClient;  //与ResourceManagerService通讯的客户端，负责资源请求
    private final NMClient mNMClient;   // 与NN通讯的客户端
    private final YarnClient mYarnClient;   // 与ResourceManager通讯的客户端，负责RM状态信息获取
    private String mMasterContainerNetAddress;
    private CountDownLatch mOutstandingWorkerContainerRequestsLatch = null;  //统计Worker是否都完成了
    /** Security tokens for HDFS. */
    private ByteBuffer mAllTokens;

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
        options.addOption("resource_path", true,
                "(Required) HDFS path containing the Application Master");
        try {
            final CommandLine cliParser = new GnuParser().parse(options, args);
            if (UserGroupInformation.isSecurityEnabled()) {
                String user = System.getenv("USER");
                UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
                for (Token token : UserGroupInformation.getCurrentUser().getTokens()) {
                    ugi.addToken(token);
                }
                LOG.info("UserGroupInformation: " + ugi);
                ugi.doAs(new PrivilegedExceptionAction<Void>() {
                    @Override
                    public Void run() throws Exception {
                        runApplicationMaster(cliParser);
                        return null;
                    }
                });
            } else {
                runApplicationMaster(cliParser);
            }
        } catch (Exception e) {
            LOG.error("Error running Application Master", e);
            System.out.println("Error running Application Master" + e);
            System.exit(1);
        }
    }

    /**
     * Run the application master.
     */
    private static void runApplicationMaster(final CommandLine cliParser) throws Exception {
        int numWorkers = Integer.parseInt(cliParser.getOptionValue("num_workers", "1"));
        String masterAddress = cliParser.getOptionValue("master_address");
        String resourcePath = cliParser.getOptionValue("resource_path");

        ApplicationMaster applicationMaster =
                new ApplicationMaster(numWorkers, masterAddress, resourcePath);
        applicationMaster.init(new YarnConfiguration());
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

        super("YarnDemoAppMaster");
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
    protected void serviceStart() throws YarnException, IOException {
        if (UserGroupInformation.isSecurityEnabled()) {
            Credentials credentials =
                    UserGroupInformation.getCurrentUser().getCredentials();
            DataOutputBuffer credentialsBuffer = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(credentialsBuffer);
            // Now remove the AM -> RM token so that containers cannot access it.
            Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
            while (iter.hasNext()) {
                Token<?> token = iter.next();
                if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                    iter.remove();
                }
            }
            mAllTokens = ByteBuffer.wrap(credentialsBuffer.getData(), 0, credentialsBuffer.getLength());
        }
        mNMClient.init(mYarnConf);
        mNMClient.start();

        mRMClient.init(mYarnConf);
        mRMClient.start();

        mYarnClient.init(mYarnConf);
        mYarnClient.start();

        // Register with ResourceManager
        String hostname = InetAddress.getLocalHost().toString();
        mMasterContainerNetAddress = hostname;
        mRMClient.registerApplicationMaster(hostname, 0 /* port */, "" /* tracking url */);
        System.out.println("ApplicationMaster "+  hostname+"  registered," );
        LOG.info("ApplicationMaster {}  registered,", hostname);
    }

    public void requestContainers() throws Exception {
        while (mWorkerHosts.size() < mNumWorkers) {  // 一直请求，直到请求到足够的Worker
            requestWorkerContainers();
            LOG.info("Waiting for {} worker containers to be allocated",
                    mOutstandingWorkerContainerRequestsLatch.getCount());

            mOutstandingWorkerContainerRequestsLatch.await();
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

        Map<String, String> args = new HashMap<String, String>();
        args.put("ls", "/tmp");  // TODO 每个Worker的执行命令写死，后期考虑客户端传入
        final String command = YarnUtils.buildCommand(YarnUtils.YarnContainerType.WORKER, args);

        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        ctx.setCommands(Lists.newArrayList(command));
        ctx.setLocalResources(setupLocalResources(mResourcePath));
        ctx.setEnvironment(setupWorkerEnvironment(mMasterContainerNetAddress));

        if (UserGroupInformation.isSecurityEnabled()) {
            ctx.setTokens(mAllTokens.duplicate());
        }
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
        System.out.println("Requesting worker containers");
        LOG.info("Requesting worker containers");
        // Resource requirements for worker containers
        Resource workerResource = Records.newRecord(Resource.class);
        workerResource.setMemory(mWorkerMemInMB);
        workerResource.setVirtualCores(mWorkerCpu);
        int currentNumWorkers = mWorkerHosts.size();
        int neededWorkers = mNumWorkers - currentNumWorkers;

        mOutstandingWorkerContainerRequestsLatch = new CountDownLatch(neededWorkers);
        String[] hosts;
        boolean relaxLocality = false; // 严格按照请求获取资源
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

    /**
     * 获得没有填充满的节点列表
     * @return
     * @throws Exception
     */
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

    protected void serviceStop() {
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

    private static Map<String, LocalResource> setupLocalResources(String resourcePath)  {
        try {
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
            localResources.put(YarnUtils.SETUP_SCRIPT,
                    YarnUtils.createLocalResourceOfFile(new YarnConfiguration(), resourcePath + "/" + YarnUtils.SETUP_SCRIPT));
            localResources.put("iwantfind-1.0-SNAPSHOT.jar",
                    YarnUtils.createLocalResourceOfFile(new YarnConfiguration(), resourcePath + "/iwantfind-1.0-SNAPSHOT.jar"));
            localResources.put("log4j.properties",
                    YarnUtils.createLocalResourceOfFile(new YarnConfiguration(), resourcePath + "/log4j.properties"));
            return localResources;
        } catch (IOException e) {
            throw new RuntimeException("Cannot find resource", e);
        }

    }

    private static Map<String, String> setupWorkerEnvironment(String masterContainerNetAddress) {
        Map<String, String> env = new HashMap<String, String>();
        env.put(Constant.APP_HOME, masterContainerNetAddress);

        return env;
    }
}
