package org.zenoss.app.consumer.metric;

import java.util.Collection;

import org.zenoss.app.consumer.metric.data.Metric;

/**
 * Provides information on TSDB metrics that are currently being processed.
 */
public interface TsdbMetricsQueue {

    public static final String CLIENT_TAG = "x-metric-consumer-client-id";

    /**
     * How many metrics are currently queued for delivery?
     * @return total
     */
    long getTotalInFlight();

    /**
     * How many metrics does the queue currently contain tagged for the given clientId?
     * @param clientId A value that might be used to tag the client on a metric. {@see #CLIENT_TAG}
     * @return zero, or a positive number if the queue contains any metrics with a matching tag.
     */
     long clientBacklogSize(String clientId);
    
    /**
     * Retrieves and removes a number of elements from the queue. If there are
     * not enough elements in the queue to satisfy the request, then the entire
     * contents of the queue will be returned.
     *
     * @param size desired number elements to retrieve
     * @param maxWaitMillis max time to wait if the queue is initially empty
     * @return removed elements
     */
    Collection<Metric> poll(int size, long maxWaitMillis) throws InterruptedException;
    
    /**
     * Add elements to the queue.
     * @param metrics added elements
     * @param clientId an identifier for the remote client that is adding the metrics.
     */
    void addAll(Collection<Metric> metrics, String clientId);

    /**
     * Add elements back in to the queue, without affecting incoming totals.
     * This should only be used to reinsert elements that were {@link #poll(int, long)}'d out of the queue.
     * @param metrics added elements
     */
    void reAddAll(Collection<Metric> metrics);

    /**
     * Record a number of errors encountered.
     * @param size number of new errors
     */
    void incrementError(int size);

    /**
     * Record a number of elements as having been processed.
     * @param processed number of elements processed
     */
    void incrementProcessed(long processed);
}
