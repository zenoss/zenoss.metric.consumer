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

/**
 * Provides information on TSDB metrics that are currently being processed.
 */
public interface TsdbMetricsQueue {
    
    /**
     * How many metrics are currently queued for delivery?
     * @return total
     */
    long getTotalInFlight();
}
