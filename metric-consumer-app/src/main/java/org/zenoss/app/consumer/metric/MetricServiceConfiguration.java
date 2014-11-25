
package org.zenoss.app.consumer.metric;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.zenoss.lib.tsdb.OpenTsdbClientPoolConfiguration;

import javax.validation.Valid;

import org.zenoss.app.consumer.metric.data.Control.Type;

public class MetricServiceConfiguration {

    @Valid
    @JsonProperty("openTsdbClientPool")
    private OpenTsdbClientPoolConfiguration openTsdbClientPoolConfiguration = new OpenTsdbClientPoolConfiguration();

    /**
     * how many metrics per thread
     */
    @JsonProperty
    private int jobSize = 5;

    /**
     * How many queued messages before throttling/dropping metrics from all clients and sending high collisions
     */
    @JsonProperty
    private int highCollisionMark = 1024;

    /**
     * How many queued messages before throttling/dropping metrics from backlogged clients and sending low collisions
     * Defaults to 90% of {@link #highCollisionMark}.
     */
    @JsonProperty
    private int lowCollisionMark = -1;

    /**
     * minimum time in milliseconds between broadcasting backoff messages
     */
    @JsonProperty
    private int minTimeBetweenBroadcast = 500;

    /**
     * Max time in milliseconds to have a throttled client wait to add some metrics to a backlogged queue
     * before we decide to just give up instead.
     */
    @JsonProperty
    private int maxClientWaitTime = 60000;

    /**
     * Max time in milliseconds with no work before TSDB writer threads will commit seppuku
     */
    @JsonProperty
    private int maxIdleTime = 10000;

    /**
     * Max time in milliseconds to wait for reconnecting when no connections available
     */
    @JsonProperty
    private int maxConnectionBackOff = 5000;

    /**
     * Min time in milliseconds to wait for reconnecting when no connections available
     */
    @JsonProperty
    private int minConnectionBackOff = 100;

    /**
     * How many queued messages per client, before throttling.
     * Defaults to 80% of {@link #highCollisionMark}.
     */
    @JsonProperty
    private int perClientMaxBacklogSize = -1;

    /**
     * Ideal number of TSDB writer threads
     */
    @JsonProperty
    private int tsdbWriterThreads = 1;

    /**
     * Size of tsdb writer thread pool
     */
    @JsonProperty
    private int threadPoolSize = 10;

    @JsonProperty
    private String consumerName = "Consumer";

    @JsonProperty
    private int selfReportFrequency = 0; // Zero means no reporting

    /**
     * TSDB client pool configuration.
     *
     * @return config
     */
    public OpenTsdbClientPoolConfiguration getOpenTsdbClientPoolConfiguration() {
        return openTsdbClientPoolConfiguration;
    }

    /**
     * The name of this consumer. This should be unique per JVM.
     *
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
     *
     * @return batch size
     */
    public int getJobSize() {
        return jobSize;
    }

    /**
     * Number of metrics in the backlog used to warn that metrics are being
     * queued faster than they can be processed. Once this threshold has been
     * reached, no further metrics will be accepted from clients that are already
     * in the backlog until some have been written to TSDB. This will also cause
     * {@link Type#LOW_COLLISION} messages to be broadcast to any connected
     * websocket clients.
     *
     * @return threshold
     */
    public int getLowCollisionMark() {
        if (lowCollisionMark <= 0)
            return highCollisionMark * 9 / 10;
        else
            return lowCollisionMark;
    }

    /**
     * The maximum time to have a throttled client wait to add some metrics to a backlogged queue before we decide
     * to just give up instead.
     *
     * @return milliseconds
     */
    public int getMaxClientWaitTime() {
        return maxClientWaitTime;
    }

    /**
     * The maximum time to wait before trying to get a new connection when one isn't available
     *
     * @return milliseconds
     */
    public int getMaxConnectionBackOff() {
        return maxConnectionBackOff;
    }

    /**
     * The maximum time TSDB writer threads should wait while there is no work
     * to do.
     *
     * @return milliseconds
     */
    public int getMaxIdleTime() {
        return maxIdleTime;
    }

    /**
     * The minimum time to wait before trying to get a new connection when one isn't available
     *
     * @return milliseconds
     */
    public int getMinConnectionBackOff() {
        return minConnectionBackOff;
    }

    /**
     * The minimum time to wait before broadcasting the same message to
     * connected websocket clients. This prevents sending the same message
     * before a client can process the first message.
     *
     * @return milliseconds
     */
    public int getMinTimeBetweenBroadcast() {
        return minTimeBetweenBroadcast;
    }


    /**
     * A threshold number of metrics in the backlog from a single client, after
     * which no further metrics will be accepted from that clients until some
     * have been written to TSDB.
     *
     * @return threshold
     */
    public int getPerClientMaxBacklogSize() {
        if (perClientMaxBacklogSize <= 0)
            return highCollisionMark * 8 / 10;
        else
            return perClientMaxBacklogSize;
    }


    /**
     * The frequency with which this application will report internal metrics
     * on throughput to TSDB. If this is less than or equal zero the reporter
     * will be disabled.
     *
     * @return time in milliseconds
     */
    public int getSelfReportFrequency() {
        return selfReportFrequency;
    }

    /**
     * The minimum size of the general purpose thread pool for this application
     *
     * @return size
     */
    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    /**
     * Number of background threads that will simultaneously write to TSDB.
     *
     * @return threads
     */
    public int getTsdbWriterThreads() {
        return tsdbWriterThreads;
    }

    /**
     * The name of this consumer. This should be unique per JVM.
     *
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
        this.highCollisionMark = highCollisionMark;
    }

    /**
     * The batch size to use when writing metrics to TSDB.
     *
     * @param jobSize batch size
     */
    public void setJobSize(int jobSize) {
        this.jobSize = jobSize;
    }

    /**
     * Number of metrics in the backlog used to warn that metrics are being
     * queued faster than they can be processed. Once this threshold has been
     * reached, no further metrics will be accepted from clients that are already
     * in the backlog until some have been written to TSDB. This will also cause
     * {@link Type#LOW_COLLISION} messages to be broadcast to any connected
     * websocket clients.
     *
     * @param lowCollisionMark threshold
     */
    public void setLowCollisionMark(int lowCollisionMark) {
        this.lowCollisionMark = lowCollisionMark;
    }

    /**
     * The maximum time to have a throttled client wait to add some metrics to a backlogged queue before we decide
     * to just give up instead.

     * @param maxClientWaitTime milliseconds
     */
    public void setMaxClientWaitTime(int maxClientWaitTime) {
        this.maxClientWaitTime = maxClientWaitTime;
    }

    /**
     * The maximum time TSDB writer threads should wait while there is no work
     * to do.
     *
     * @param maxIdleTime milliseconds
     */
    public void setMaxIdleTime(int maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    /**
     * The minimum time to wait before broadcasting the same message to
     * connected websocket clients. This prevents sending the same message
     * before a client can process the first message.
     *
     * @param minTimeBetweenBroadcast milliseconds
     */
    public void setMinTimeBetweenBroadcast(int minTimeBetweenBroadcast) {
        this.minTimeBetweenBroadcast = minTimeBetweenBroadcast;
    }

    /**
     * A threshold number of metrics in the backlog from a single client, after
     * which no further metrics will be accepted from that clients until some
     * have been written to TSDB.
     *
     * @param perClientMaxBacklogSize threshold
     */
    public void setPerClientMaxBacklogSize(int perClientMaxBacklogSize) {
        this.perClientMaxBacklogSize = perClientMaxBacklogSize;
    }

    /**
     * TSDB client pool configuration.
     *
     * @param openTsdbClientPoolConfiguration
     *         config
     */
    public void setOpenTsdbClientPoolConfiguration(OpenTsdbClientPoolConfiguration openTsdbClientPoolConfiguration) {
        this.openTsdbClientPoolConfiguration = openTsdbClientPoolConfiguration;
    }

    /**
     * The frequency with which this application will report internal metrics
     * on throughput to TSDB. If this is less than or equal zero the reporter
     * will be disabled.
     *
     * @param milliseconds time in milliseconds
     */
    public void setSelfReportFrequency(int milliseconds) {
        this.selfReportFrequency = milliseconds;
    }

    /**
     * Time to sleep between checking for work when the metric backlog is empty.
     *
     * @param numberOfThreads threads
     */
    public void setTsdbWriterThreads(int numberOfThreads) {
        this.tsdbWriterThreads = numberOfThreads;
    }


}
