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

import java.util.Collection;

import org.zenoss.app.consumer.metric.data.Metric;

/**
 * Provides information on TSDB metrics that are currently being processed.
 */
public interface TsdbMetricsQueue {
    
    /**
     * How many metrics are currently queued for delivery?
     * @return total
     */
    long getTotalInFlight();
    
    /**
     * Retrieves and removes a number of elements from the queue. If there are
     * not enough elements in the queue to satisfy the request, then the entire
     * contents of the queue will be returned.
     * @param size
     * @return removed elements
     */
    Collection<Metric> poll(int size);
    
    /**
     * Add elements to the queue.
     * @param metrics added elements
     */
    void addAll(Collection<Metric> metrics);
}
