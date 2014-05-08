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

import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.TsdbMetricsQueue;
import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.TsdbWriterRegistry;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.lib.tsdb.OpenTsdbClientPool;

import java.util.Collection;

/**
 * @see TsdbWriter
 */
@Component
@Profile("dev")
@Scope("prototype")
class FakeTsdbWriter extends OpenTsdbWriter {

    @Autowired
    FakeTsdbWriter(
            MetricServiceConfiguration config,
            TsdbWriterRegistry registry,
            OpenTsdbClientPool clientPool,
            TsdbMetricsQueue metricsQueue,
            EventBus eventBus) {
        super(config, registry, clientPool, metricsQueue, eventBus);
    }


    void processBatch(Collection<Metric> metrics) throws InterruptedException {
        boolean flushed = false;
        try {
            long processed = 0;
            for (Metric m : metrics) {
                String message = convert(m);
                log.debug("Put msg: {}", message);
                processed++;
                if (processed % 10 == 0) Thread.sleep(1);
            }
            if (processed > 0)
                log.info("Pretended to write {} messages", processed);
            flushed = true;
            metricsQueue.incrementProcessed(processed);
        } finally {
            if (!flushed) {
                metricsQueue.reAddAll(metrics);
            }
            lastWorkTime = System.currentTimeMillis();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(FakeTsdbWriter.class);
}
