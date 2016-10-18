/**
 * 
 */
package org.zenoss.app.consumer.metric;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.databus.common.utils.PathUtils;
import org.zenoss.databus.common.utils.PropertiesUtils;
import org.zenoss.databus.common.utils.StringUtils;
import org.zenoss.databus.producer.DatabusProducerConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.io.Resources;

/**
 * 
 *
 */
public class DatabusPublishConfig {
private static final Logger logger = LoggerFactory.getLogger(DatabusPublishConfig.class);

private static final String CONFIG_FILE_KEY = "configFile";
private static final String DIR_LIST_KEY = "directoryList";

@JsonProperty
private String configFile = "";
private List<Path> directoryPaths = null;
private List<String> directoryList = null;
private DatabusProducerConfig defaultConfig = null;
@JsonProperty
private int minThreadPoolSize = 3;
@JsonProperty
private int maxThreadPoolSize = 10;
@JsonProperty
private int maxIdleTime = 30000;
@JsonProperty
private int databusWriterThreads = 1;
@JsonProperty
private int batchSize = 5;
@JsonProperty
private boolean enablePublish = true;

public DatabusPublishConfig() {
    this.directoryList = new ArrayList<>();
    this.defaultConfig = null;

}


@JsonProperty
public String getConfigFile() {
    return this.configFile;
}

public DatabusProducerConfig getDefaultConfig() {
    if (this.defaultConfig == null) {
        initConfig();
    }
    return this.defaultConfig;
}

@JsonProperty
public List<String> getDirectoryList() {
    return this.directoryList;
}

public List<Path> getDirectoryPaths() {
    if (this.directoryPaths == null) {
        directoryPaths = new ArrayList<>();
        for (String dir : this.directoryList) {
            directoryPaths.add(Paths.get(dir));
        }
    }
    return this.directoryPaths;
}

public Properties getProperties() {
    return getDefaultConfig().getProperties();
}

public String getProperty(String key) {
    return getDefaultConfig().getProperty(key);
}



public int getMinThreadPoolSize() {
	return minThreadPoolSize;
}


public void setMinThreadPoolSize(int minThreadPoolSize) {
	this.minThreadPoolSize = minThreadPoolSize;
}


public int getMaxThreadPoolSize() {
	return maxThreadPoolSize;
}


public void setMaxThreadPoolSize(int maxThreadPoolSize) {
	this.maxThreadPoolSize = maxThreadPoolSize;
}


public int getMaxIdleTime() {
	return maxIdleTime;
}


public void setMaxIdleTime(int maxIdleTime) {
	this.maxIdleTime = maxIdleTime;
}


public int getDatabusWriterThreads() {
	return databusWriterThreads;
}


public void setDatabusWriterThreads(int databusWriterThreads) {
	this.databusWriterThreads = databusWriterThreads;
}


public int getBatchSize() {
	return batchSize;
}


public void setBatchSize(int batchSize) {
	this.batchSize = batchSize;
}


public boolean isEnablePublish() {
	return enablePublish;
}


public void setEnablePublish(boolean enablePublish) {
	this.enablePublish = enablePublish;
}


@JsonProperty
public void setConfigFile(String configFile) {
    this.configFile = configFile;
    this.defaultConfig = null;
    //
}

public void setDefaultConfig(DatabusProducerConfig defaultConfig) {
    this.defaultConfig = defaultConfig;
    initConfig();
}

@JsonProperty
public void setDirectoryList(List<String> directoryList) {
    this.directoryList = directoryList;
    this.directoryPaths = null;
}

public void setDirectoryPaths(List<Path> directoryList) {
    this.directoryPaths = directoryList;
}

public void setProperties(Map<String, String> oprops) {
    getDefaultConfig().setProperties(oprops);
}

public void setProperty(String key, String value) {
    getDefaultConfig().setProperty(key, value);
}

private void initConfig() {
    // check for override of configFile
    String opt = PropertiesUtils.getPropertyWithPrecedence(CONFIG_FILE_KEY, Optional.of(CONFIG_FILE_KEY),
            Optional.empty());
    if (!StringUtils.isNullOrEmpty(opt)) {
        logger.debug("Overriding {} from the system properties/env", CONFIG_FILE_KEY);
        this.configFile = opt;
        opt = ""; // NOSONAR
    }

    opt = PropertiesUtils.getPropertyWithPrecedence(DIR_LIST_KEY, Optional.of(DIR_LIST_KEY), Optional.empty());
    if (!StringUtils.isNullOrEmpty(opt)) {
        logger.debug("Overriding {} from the system properties/env", DIR_LIST_KEY);
        this.directoryList = StringUtils.delimitedString2list(opt, ",");
        opt = ""; // NOSONAR
    }
    //
    if (this.defaultConfig == null) {
        //
        Path configPath = PathUtils.resolvePath(configFile, getDirectoryPaths(), ".");
        if (!Files.exists(configPath)) {
            this.configFile = Resources.getResource(configFile).getPath();
        } else {
            this.configFile = configPath.toString();
        }

        logger.debug("Building default config from: {}", configFile);
        defaultConfig = new DatabusProducerConfig(this.configFile);
    }
    // Add local overrides
//    String dirString = StringUtils.list2DelimitedString(directoryList, ',');

//    defaultConfig.setProperty(DataPipelineConstants.APPLICATION_DIR_PATH, dirString);
//
//    defaultConfig.setProperty(DataPipelineConstants.APPLICATION_CONFIG_PATH, this.configFile);

}



}
