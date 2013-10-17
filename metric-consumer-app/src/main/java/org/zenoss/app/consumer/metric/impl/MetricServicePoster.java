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
package org.zenoss.app.consumer.metric.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.metrics.reporter.MetricBatch;
import org.zenoss.metrics.reporter.MetricPoster;

import java.io.IOException;

/**
 * Report internal metrics to TSD.
 */
@Component
class MetricServicePoster implements MetricPoster {

    private final MetricService metricService;

    @Autowired
    MetricServicePoster(MetricService metricService) {
        this.metricService = metricService;
    }

    @Override
    public void post(MetricBatch batch) throws IOException {
        metricService.push(batch.getMetrics());
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void start() {

    }

}
