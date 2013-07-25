/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss under the directory where your Zenoss product is installed.
 *
 * ***************************************************************************
 */

package org.zenoss.app.consumer.metric;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.zenoss.lib.tsdb.OpenTsdbClientPoolConfiguration;

import javax.validation.Valid;

public class MetricServiceConfiguration {

    @Valid
    @JsonProperty("openTsdbClientPool")
    private OpenTsdbClientPoolConfiguration openTsdbClientPoolConfiguration = new OpenTsdbClientPoolConfiguration();

    /** how many concurrent threads for publishing to opentsdb  */
    @JsonProperty
    private int maxThreads = 1;

    /** how many metrics per thread */
    @JsonProperty
    private int jobSize = 5;

    /** how many seconds to wait for thread termination on shutdown */
    @JsonProperty
    private int terminationTimeout = 60;

    /** how many queued messages before dropping metrics and sending high collisions */
    @JsonProperty
    private int highCollisionMark = 1024;

    /** how many queued messages before sending low collisions */
    @JsonProperty
    private int lowCollisionMark = 512;
    
    /** minimum time in milliseconds between broadcasting backoff messages */
    @JsonProperty
    private int minTimeBetweenBroadcast = 500;

    public OpenTsdbClientPoolConfiguration getOpenTsdbClientPoolConfiguration() {
        return openTsdbClientPoolConfiguration;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public int getJobSize() {
        return jobSize;
    }

    public int getTerminationTimeout() {
        return terminationTimeout;
    }

    public int getHighCollisionMark() {
        return highCollisionMark;
    }

    public int getLowCollisionMark() {
        return lowCollisionMark;
    }
    
    public int getMinTimeBetweenBroadcast() {
        return minTimeBetweenBroadcast;
    }

    public void setOpenTsdbClientPoolConfiguration(OpenTsdbClientPoolConfiguration openTsdbClientPoolConfiguration) {
        this.openTsdbClientPoolConfiguration = openTsdbClientPoolConfiguration;
    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    public void setJobSize(int jobSize) {
        this.jobSize = jobSize;
    }

    public void setTerminationTimeout(int terminationTimeout) {
        this.terminationTimeout = terminationTimeout;
    }

    public void setHighCollisionMark(int highCollisionMark) {
        this.highCollisionMark= highCollisionMark;
    }

    public void setLowCollisionMark(int lowCollisionMark) {
        this.lowCollisionMark= lowCollisionMark;
    }

    public void setMinTimeBetweenBroadcast(int minTimeBetweenBroadcast) {
        this.minTimeBetweenBroadcast = minTimeBetweenBroadcast;
    }
}
