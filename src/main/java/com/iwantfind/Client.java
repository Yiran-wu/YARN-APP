package com.iwantfind;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.cli.*;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by YiRan on 12/13/16.
 */
public class Client {

    /** Yarn client 与ResourceManager会话. */
    private YarnClient mYarnClient;
    /** Yarn configuration. */
    private YarnConfiguration mYarnConf = new YarnConfiguration();
    /** Container context ApplicationMaster 启动环境类 */
    private ContainerLaunchContext mAmContainer;
    /** ApplicationMaster 提交信息类. */
    private ApplicationSubmissionContext mAppContext;
    /** Application name. */
    private String mAppName;
    /** ApplicationMaster priority. */
    private int mAmPriority;
    /** Queue for ApplicationMaster. */
    private String mAmQueue;
    /** Id of the application. */
    private ApplicationId mAppId;
    /** Command line options. */
    private Options mOptions;
    /** Amount of memory to request for running the ApplicationMaster. */
    private int mAmMemoryInMB;
    /** Number of virtual cores to request for running the ApplicationMaster. */
    private int mAmVCores;
    /** Number of workers. */
    private int mNumWorkers;
    /** ApplicationMaster jar file on HDFS. */
    private String mResourcePath;

    private final String YARN_NOT_ENOUGH_RESOURCES =
      "{0} {1} specified above max threshold of cluster, specified={2}, max={3}";

    public Client () {
        mOptions = new Options();
        mOptions.addOption("appname", true, "Application Name.");
        mOptions.addOption("priority", true, "Application Priority. Default 0");
        mOptions.addOption("queue", true,
                "RM Queue in which this application is to be submitted. Default 'default'");
        mOptions.addOption("am_memory", true,
                "Amount of memory in MB to request to run ApplicationMaster. Default 256");
        mOptions.addOption("am_vcores", true,
                "Amount of virtual cores to request to run ApplicationMaster. Default 1");
//        mOptions.addOption("master_address", true, "(Required) Address to run Alluxio master");
        mOptions.addOption("help", false, "Print usage");
        mOptions.addOption("num_workers", true, "Number of Alluxio workers to launch. Default 1");
        mOptions.addOption("resource_path", true,
                "(Required) HDFS path containing the Application Master");
    }
    public Client(String[] args) throws ParseException {
        this();
        parseArgs(args);
    }

    public static void main(String[] args) {
        try {
            Client client = new Client();
            System.out.println("Initializing Client");
            if (!client.parseArgs(args)) {
                System.out.println("Cannot parse commandline: " + Arrays.toString(args));
                System.exit(0);
            }
            System.out.println("Starting Client");
            client.run();
        } catch (Exception e) {
            System.err.println("Error running Client " + e);
            System.exit(1);
        }
    }

    private void printUsage() {
        new HelpFormatter().printHelp("Client", mOptions);
    }

    private boolean parseArgs(String[] args) throws ParseException {
        Preconditions.checkArgument(args.length > 0, "No args specified for client to initialize");
        CommandLine cliParser = new GnuParser().parse(mOptions, args);

        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }
        if (!cliParser.hasOption("resource_path")) {
            System.out.println("Required to specify resource_path");
            printUsage();
            return false;
        }

        mAppName = cliParser.getOptionValue("appname", "YarnAppDemo");
        mAmPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        mAmQueue = cliParser.getOptionValue("queue", "default");
        mAmMemoryInMB = Integer.parseInt(cliParser.getOptionValue("am_memory", "256"));
        mAmVCores = Integer.parseInt(cliParser.getOptionValue("am_vcores", "1"));
        mNumWorkers = Integer.parseInt(cliParser.getOptionValue("num_workers", "1"));

        Preconditions.checkArgument(mAmMemoryInMB > 0,
                "Invalid memory specified for application master, " + "exiting. Specified memory="
                        + mAmMemoryInMB);
        Preconditions.checkArgument(mAmVCores > 0,
                "Invalid virtual cores specified for application master, exiting."
                        + " Specified virtual cores=" + mAmVCores);
        return true;
    }

    public void run() throws YarnException, IOException, InterruptedException {
        this.submitApplication();
    }

    private  void submitApplication() throws YarnException, IOException, InterruptedException {
        // 1. 初始化YarnClient
        mYarnClient = YarnClient.createYarnClient();
        mYarnClient.init(mYarnConf);
        mYarnClient.start();

        // 2. 创建一个Application
        YarnClientApplication app = mYarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        // 3.检查集群是否有足够的资源启动AppMaster
        checkClusterResource(appResponse);

        // 4. 给AppMaster 设置 container launch context
        mAmContainer = Records.newRecord(ContainerLaunchContext.class);
        setupContainerLaunchContext();

        // 5. 设置Application的提交属性信息， appname, priority ...
        mAppContext = app.getApplicationSubmissionContext();
        setupApplicationSubmissionContext();

        // 6. 提交作业到集群并监控其结束
        mAppId = mAppContext.getApplicationId();
        System.out.println("Submitting application of id " + mAppId + " to ResourceManager");
        mYarnClient.submitApplication(mAppContext);
        monitorApplication();
    }

    /**
     * 检查集群是否有资源启动AppMaster
     * @param appResponse
     */
    private void checkClusterResource(GetNewApplicationResponse appResponse) {
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();

        if (mAmMemoryInMB > maxMem) {
            throw new RuntimeException(YARN_NOT_ENOUGH_RESOURCES
                    .format("ApplicationMaster", "memory", mAmMemoryInMB, maxMem));
        }

        if (mAmVCores > maxVCores) {
            throw new RuntimeException(YARN_NOT_ENOUGH_RESOURCES
                    .format("ApplicationMaster", "virtual cores", mAmVCores, maxVCores));
        }
    }

    /**
     * 设置ContainerLaunchContext
     * @throws IOException
     * @throws YarnException
     */
    private void setupContainerLaunchContext() throws IOException, YarnException {
        Map<String, String> applicationMasterArgs = ImmutableMap.<String, String>of(
                "-num_workers", Integer.toString(mNumWorkers));

        final String amCommand =
                YarnUtils.buildCommand(YarnUtils.YarnContainerType.APPLICATION_MASTER, applicationMasterArgs);

        System.out.println("ApplicationMaster command: " + amCommand);
        mAmContainer.setCommands(Collections.singletonList(amCommand));

        // Setup local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        localResources.put(YarnUtils.SETUP_SCRIPT,
                YarnUtils.createLocalResourceOfFile(mYarnConf, mResourcePath + "/" + YarnUtils.SETUP_SCRIPT));
        mAmContainer.setLocalResources(localResources);

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv);
        mAmContainer.setEnvironment(appMasterEnv);
    }

    /**
     * 设置AppMaster的环境变量
     * @param appMasterEnv
     * @throws IOException
     */
    private void setupAppMasterEnv(Map<String, String> appMasterEnv) throws IOException {
        String classpath = ApplicationConstants.Environment.CLASSPATH.name();
        for (String path : mYarnConf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, classpath, path.trim(),
                    ApplicationConstants.CLASS_PATH_SEPARATOR);
        }
        Apps.addToEnvironment(appMasterEnv, classpath, ApplicationConstants.Environment.PWD.$(),
                ApplicationConstants.CLASS_PATH_SEPARATOR);

        appMasterEnv.put("YARN_APP_DEMO_HOME", ApplicationConstants.Environment.PWD.$());

    }

    /**
     * 设置Application 的提交信息
     */
    private void setupApplicationSubmissionContext() {
        mAppContext.setApplicationName(mAppName);  // App name
        Resource capability = Resource.newInstance(mAmMemoryInMB, mAmVCores); // mem , vcore
        mAppContext.setResource(capability);
        mAppContext.setQueue(mAmQueue);  // queue
        mAppContext.setAMContainerSpec(mAmContainer);  // ContainerLaunchContext
        mAppContext.setPriority(Priority.newInstance(mAmPriority));  // priority
    }

    /**
     * 监控应用程序的运行状态  running, finished, killed or failed.
     */
    private void monitorApplication() throws YarnException, IOException, InterruptedException {
        while (true) {
            Thread.sleep(5000);
            //根据AppID 获得作业状态
            ApplicationReport report = mYarnClient.getApplicationReport(mAppId);

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            switch (state) {
                case RUNNING:
                    System.out.println("Application is running. Tracking url is " + report.getTrackingUrl());
                    return;
                case FINISHED:
                    if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                        System.out.println("Application has completed successfully");
                    } else {
                        System.out.println("Application finished unsuccessfully. YarnState="
                                + state.toString() + ", DSFinalStatus=" + dsStatus.toString());
                    }
                    return;
                case KILLED: // intended to fall through
                case FAILED:
                    System.out.println("Application did not finish. YarnState=" + state.toString()
                            + ", DSFinalStatus=" + dsStatus.toString());
                    return;
                default:
                    System.out.println("Application is in state " + state + ". Waiting.");
            }
        }
    }
}
