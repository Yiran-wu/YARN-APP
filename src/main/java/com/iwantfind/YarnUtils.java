package com.iwantfind;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
/**
 * Created by YiRan on 12/13/16.
 */
public class YarnUtils {

    public static final String SETUP_SCRIPT = "run.sh";

    public static LocalResource createLocalResourceOfFile(YarnConfiguration yarnConf,
                                                          String resource) throws IOException {
        LocalResource localResource = Records.newRecord(LocalResource.class);

        Path resourcePath = new Path(resource);

        FileStatus jarStat = FileSystem.get(resourcePath.toUri(), yarnConf).getFileStatus(resourcePath);
        localResource.setResource(ConverterUtils.getYarnUrlFromPath(resourcePath));
        localResource.setSize(jarStat.getLen());
        localResource.setTimestamp(jarStat.getModificationTime());
        localResource.setType(LocalResourceType.FILE);
        localResource.setVisibility(LocalResourceVisibility.PUBLIC);
        return localResource;
    }
    public enum YarnContainerType {
        APPLICATION_MASTER("application-master"),
        WORKER("worker"),;
        private final String mName;

        YarnContainerType(String name) {
            mName = name;
        }

        /**
         * @return the name of the container type
         */
        public String getName() {
            return mName;
        }
    }

    public static String buildCommand(YarnContainerType containerType) {
        return buildCommand(containerType, new HashMap<String, String>());
    }

    public static String buildCommand(YarnContainerType containerType, Map<String, String> args) {
        CommandBuilder commandBuilder =
                new CommandBuilder("./" + SETUP_SCRIPT).addArg(containerType.getName());
        for (Map.Entry<String, String> argsEntry : args.entrySet()) {
            commandBuilder.addArg(argsEntry.getKey(), argsEntry.getValue());
        }
        // Redirect stdout and stderr to yarn log files
        commandBuilder.addArg("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        commandBuilder.addArg("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        return commandBuilder.toString();
    }


}
