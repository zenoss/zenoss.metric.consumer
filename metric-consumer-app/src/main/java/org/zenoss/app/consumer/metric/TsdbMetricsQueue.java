/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */
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
     * Count how many unique clients have we seen recently and/or currently have metrics in the queue?
     */
    long clientCount();

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

    /**
     * Record a number of metrics were lost.
     * @param size number of lost metrics.
     */
    void incrementLostMetrics(long size);

    /**
     * Record a number of metrics were received (but not necessarily accepted).
     * @param received number of received metrics.
     */
    void incrementReceived(long received);

    /**
     * Record a number of metrics were rejected (after being received).
     * @param rejected number of rejected metrics.
     */
    void incrementRejected(long rejected);

    /**
     * Record a high collision event.
     */
    void incrementHighCollision();

    /**
     * Record a low collision event.
     */
    void incrementLowCollision();

    /**
     * Record a client collision event.
     */
    void incrementClientCollision();

    /**
     *  Record a client collision notification was sent via websocket.
     */
    void incrementSentClientCollision();

    /**
     * Record a low collision event was broadcast via websocket.
     */
    void incrementBroadcastLowCollision();

    /**
     * Recored a high collision event was broadcast via websocket.
     */
    void incrementBroadcastHighCollision();
}
