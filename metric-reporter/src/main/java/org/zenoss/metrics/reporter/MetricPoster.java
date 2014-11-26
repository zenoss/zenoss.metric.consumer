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
package org.zenoss.metrics.reporter;

import java.io.IOException;

/**
 * Method to send a MetricBatch to zenoss
 */
public interface MetricPoster {

    void post(MetricBatch batch) throws IOException;

    void shutdown();

    void start();
}
