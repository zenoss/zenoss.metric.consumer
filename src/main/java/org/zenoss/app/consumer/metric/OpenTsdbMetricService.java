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

import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.dropwizardspring.annotations.Managed;

@Managed
public class OpenTsdbMetricService implements MetricService, com.yammer.dropwizard.lifecycle.Managed {

    static final Logger log = LoggerFactory.getLogger(OpenTsdbMetricService.class);

    @Autowired
    public OpenTsdbMetricService(ConsumerAppConfiguration config, @Qualifier("zapp::event-bus::async") EventBus eventBus) {
        this(config, new OpenTsdbExecutorService(config.getMetricServiceConfiguration()), eventBus);
    }

    public OpenTsdbMetricService(ConsumerAppConfiguration config, OpenTsdbExecutorService executorService, EventBus eventBus) {
        this.config = config;
        this.executorService = executorService;
        this.eventBus = eventBus;
    }

    @Override
    public void start() {
        log.info( "OpenTsdbMetricService.start()");
    }

    @Override
    public void stop() throws InterruptedException {
        log.info( "OpenTsdbMetricService.stop()");
        executorService.stop();
    }

    @Override
    public Control push(Metric[] metrics) {
        if (metrics == null) {
            return Control.malformedRequest("metrics not nullable");
        }

        if (collides(metrics.length)) {
            return Control.dropped("collision detected");
        }

        //yee haw - all your metrics are belong to us
        int i = 0;
        int jobSize = config.getMetricServiceConfiguration().getJobSize();
        while (i < metrics.length && i + jobSize > 0) {
            int end = i + jobSize;
            if (end > metrics.length) {
                end = metrics.length;
            }
            executorService.submit(this, metrics, i, end);
            i += jobSize;
        }

        if (i < metrics.length) {
            //this may happen on overflow
            executorService.submit(this, metrics, i, metrics.length);
        }

        return Control.ok();
    }

    /**
     * @ return how many messages are in flight
     */
    public synchronized int getTotalInFlight() {
        return totalInFlight;
    }

    /**
     * @ return how many messages were queued
     */
    public synchronized long getTotalIncoming() {
        return totalIncoming;
    }

    /**
     * @ return how many messages were written
     */
    public synchronized long getTotalOutgoing() {
        return totalOutgoing;
    }

    /**
     * @ return how many errors did OpenTsdb return
     */
    public synchronized long getTotalErrors() {
        return totalErrors;
    }

    /**
     * high/low collision test and increment, broad cast control messages
     */
    synchronized boolean collides(int v) {
        if (v >= 0) {
            //overflow...
            if (totalInFlight + v < 0) {
                eventBus.post(Control.highCollision());
                return true;
            }

            int highCollision = config.getMetricServiceConfiguration().getHighCollisionMark();
            if (totalInFlight + v >= highCollision) {
                eventBus.post(Control.highCollision());
                return true;
            }

            int lowCollision = config.getMetricServiceConfiguration().getLowCollisionMark();
            if (totalInFlight + v >= lowCollision) {
                eventBus.post(Control.lowCollision());
            }
            totalInFlight += v;
            totalIncoming += v;

            //alive for awhile...
            if (totalIncoming < 0) {
                totalIncoming = 0;
            }

            return false;
        }

        return true;
    }

    synchronized void incrementTotalProcessed(int v) {
        if (v >= 0) {
            totalInFlight -= v;
            totalOutgoing += v;

            //alive for some time...
            if (totalOutgoing < 0) {
                totalOutgoing = 0;
            }
        }
    }

    synchronized void incrementTotalError(int v) {
        if (v >= 0) {
            totalErrors += v;
            //wrap around back to zero, because, opentsdb may not like our metrics :-(
            if (totalErrors < 0) {
                totalErrors = v;
            }
        }
    }

    /**
     * application configuration
     */
    private ConsumerAppConfiguration config;

    /**
     * manage opentsd read/write jobs
     */
    private OpenTsdbExecutorService executorService;

    /**
     * event bus for high/low collisions
     */
    private EventBus eventBus;

    /**
     * counter for total messages pushed
     */
    private long totalIncoming = 0;

    /**
     * counter for total messages send to OpenTsdb
     */
    private long totalOutgoing = 0;

    /**
     * counter for total messages inflight
     */
    private int totalInFlight = 0;

    /**
     * counter for total errors from OpenTsdb
     */
    private long totalErrors = 0;
}
