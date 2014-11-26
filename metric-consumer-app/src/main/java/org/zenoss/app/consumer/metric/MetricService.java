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

import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;

import java.util.List;

public interface MetricService {
    
    /**
     * Eagerly submit metrics to the tail of the queue until a high collision
     * is detected.
     *
     * @param metrics metrics to be written to TSDB
     * @return control message with result
     */
    Control push(List<Metric> metrics, String clientId);

}
