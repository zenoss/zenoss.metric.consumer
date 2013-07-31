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

import java.util.concurrent.Future;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;

public interface MetricService {
    
    /**
     * Eagerly submit metrics to the tail of the queue until a high collision 
     * is detected.
     * 
     * @param metric metrics to be written to TSDB
     * @return control message with result
     */
    Control push(Metric[] metric);
    
    /**
     * Stop a TSDB writer thread.
     * @return success
     */
    boolean stopWriter();
    
    /**
     * Start a TSDB writer thread.
     * @return Future that can be used to get status of writer thread
     */
    Future<?> startWriter();
}
