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
package org.zenoss.app.consumer.metric.remote;

import com.yammer.metrics.core.HealthCheck;
import org.springframework.beans.factory.annotation.Autowired;

import org.zenoss.app.consumer.metric.TsdbMetricsQueue;
import org.zenoss.app.consumer.metric.TsdbWriterRegistry;

/**
 *
 * @author cschellenger
 */
@org.zenoss.dropwizardspring.annotations.HealthCheck
class TsdbWriterHealthCheck extends HealthCheck {
    
    private final TsdbWriterRegistry registry;
    private final TsdbMetricsQueue queue;

    @Autowired
    TsdbWriterHealthCheck(TsdbWriterRegistry factory, TsdbMetricsQueue metricsQueue) {
        super("TSDB Writer");
        this.registry = factory;
        this.queue = metricsQueue;
    }

    @Override
    protected Result check() {
        if (queue.getTotalInFlight() > 0) {
            return registry.size() > 0? Result.healthy() : Result.unhealthy("Messages queued, but no writers are running.");
        }
        else {
            return registry.size() == 0? Result.healthy() : Result.unhealthy("No messages queued, but writers are running.");
        }
    }

    
}
