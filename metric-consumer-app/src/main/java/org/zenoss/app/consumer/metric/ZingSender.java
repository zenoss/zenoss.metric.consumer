/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2017, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */
package org.zenoss.app.consumer.metric;

import org.zenoss.app.consumer.metric.data.Metric;

import java.util.Collection;

public interface ZingSender {
    void send(Collection<Metric> metrics) throws Exception;
}
