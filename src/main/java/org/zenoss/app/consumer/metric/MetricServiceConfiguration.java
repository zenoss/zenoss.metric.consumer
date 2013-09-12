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

import org.zenoss.app.consumer.metric.data.Control.Type;

public class MetricServiceConfiguration {

    @Valid
    @JsonProperty("openTsdbClientPool")
    private OpenTsdbClientPoolConfiguration openTsdbClientPoolConfiguration = new OpenTsdbClientPoolConfiguration();

    /** how many metrics per thread */
    @JsonProperty
    private int jobSize = 5;

    /** how many queued messages before dropping metrics and sending high collisions */
    @JsonProperty
    private int highCollisionMark = 1024;

    /** how many queued messages before sending low collisions */
    @JsonProperty
    private int lowCollisionMark = 512;
    
    /** minimum time in milliseconds between broadcasting backoff messages */
    @JsonProperty
    private int minTimeBetweenBroadcast = 500;
    
    /** Max time in milliseconds with no work before TSDB writer threads will commit seppuku */
    @JsonProperty
    private int maxIdleTime = 10000;
    
    /** Ideal number of TSDB writer threads */
    @JsonProperty
    private int tsdbWriterThreads = 1;
    
    /** Size of tsdb writer thread pool */
    @JsonProperty
    private int threadPoolSize = 10;
    
    @JsonProperty
    private String consumerName = "Consumer";
    
    @JsonProperty
    private int selfReportFrequency = 0; // Zero means no reporting

    /**
     * TSDB client pool configuration.
     * @return config
     */
    public OpenTsdbClientPoolConfiguration getOpenTsdbClientPoolConfiguration() {
        return openTsdbClientPoolConfiguration;
    }
    
    /**
     * The name of this consumer. This should be unique per JVM.
     * @return consumerName
     */
    public String getConsumerName() {
        return consumerName;
    }

    /**
     * The maximum acceptable backlog of metrics. Once this threshold has been
     * reached, no further metrics will be accepted until some have been written
     * to TSDB. This will also cause {@link Type#HIGH_COLLISION} 
     * messages to be broadcast to any connected websocket clients.
     * 
     * @return threshold
     */
    public int getHighCollisionMark() {
        return highCollisionMark;
    }
    
    /**
     * The batch size to use when writing metrics to TSDB.
     * @return batch size
     */
    public int getJobSize() {
        return jobSize;
    }

    /**
     * Number of metrics in the backlog used to warn that metrics are being 
     * queued faster than they can be processed. Once this threshold has been
     * reached, {@link Type#LOW_COLLISION} messages will be broadcast 
     * to any connected websocket clients.
     * 
     * @return threshold
     */
    public int getLowCollisionMark() {
        return lowCollisionMark;
    }
    
    /**
     * The maximum time TSDB writer threads should wait while there is no work
     * to do.
     * @return milliseconds
     */
    public int getMaxIdleTime() {
        return maxIdleTime;
    }
    
    /**
     * The minimum time to wait before broadcasting the same message to 
     * connected websocket clients. This prevents sending the same message 
     * before a client can process the first message.
     * @return milliseconds
     */
    public int getMinTimeBetweenBroadcast() {
        return minTimeBetweenBroadcast;
    }
    
    /**
     * The frequency with which this application will report internal metrics
     * on throughput to TSDB. If this is less than or equal zero the reporter
     * will be disabled.
     * @return time in milliseconds
     */
    public int getSelfReportFrequency() {
        return selfReportFrequency;
    }
    
    /**
     * The minimum size of the general purpose thread pool for this application
     * @return size
     */
    public int getThreadPoolSize() {
        return threadPoolSize;
    }
    
    /**
     * Number of background threads that will simultaneously write to TSDB.
     * @return threads
     */
    public int getTsdbWriterThreads() {
        return tsdbWriterThreads;
    }
    
    /**
     * The name of this consumer. This should be unique per JVM.
     * @param consumerName unique name
     */
    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    /**
     * The maximum acceptable backlog of metrics. Once this threshold has been
     * reached, no further metrics will be accepted until some have been written
     * to TSDB. This will also cause {@link Type#HIGH_COLLISION} 
     * messages to be broadcast to any connected websocket clients.
     * 
     * @param highCollisionMark threshold
     */
    public void setHighCollisionMark(int highCollisionMark) {
        this.highCollisionMark= highCollisionMark;
    }
    
    /**
     * The batch size to use when writing metrics to TSDB.
     * @param jobSize batch size
     */
    public void setJobSize(int jobSize) {
        this.jobSize = jobSize;
    }

    /**
     * Number of metrics in the backlog used to warn that metrics are being 
     * queued faster than they can be processed. Once this threshold has been
     * reached, {@link Type#LOW_COLLISION} messages will be broadcast 
     * to any connected websocket clients.
     * 
     * @param lowCollisionMark threshold
     */
    public void setLowCollisionMark(int lowCollisionMark) {
        this.lowCollisionMark= lowCollisionMark;
    }
    
    /**
     * The maximum time TSDB writer threads should wait while there is no work
     * to do.
     * @param maxIdleTime milliseconds
     */
    public void setMaxIdleTime(int maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    /**
     * The minimum time to wait before broadcasting the same message to 
     * connected websocket clients. This prevents sending the same message 
     * before a client can process the first message.
     * @param minTimeBetweenBroadcast milliseconds
     */
    public void setMinTimeBetweenBroadcast(int minTimeBetweenBroadcast) {
        this.minTimeBetweenBroadcast = minTimeBetweenBroadcast;
    }
    
    /**
     * TSDB client pool configuration.
     * @param openTsdbClientPoolConfiguration config
     */
    public void setOpenTsdbClientPoolConfiguration(OpenTsdbClientPoolConfiguration openTsdbClientPoolConfiguration) {
        this.openTsdbClientPoolConfiguration = openTsdbClientPoolConfiguration;
    }
    
    /**
     * The frequency with which this application will report internal metrics
     * on throughput to TSDB. If this is less than or equal zero the reporter
     * will be disabled.
     * @param milliseconds time in milliseconds
     */
    public void setSelfReportFrequency(int milliseconds) {
        this.selfReportFrequency = milliseconds;
    }
    
    /**
     * Time to sleep between checking for work when the metric backlog is empty.
     * @param numberOfThreads threads
     */
    public void setTsdbWriterThreads(int numberOfThreads) {
        this.tsdbWriterThreads = numberOfThreads;
    }
    

}
