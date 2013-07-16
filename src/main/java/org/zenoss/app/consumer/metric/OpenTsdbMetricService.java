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

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.dropwizardspring.annotations.Managed;

import java.util.Arrays;
import java.util.List;

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
        highCollisionMark = config.getMetricServiceConfiguration().getHighCollisionMark();
        lowCollisionMark = config.getMetricServiceConfiguration().getLowCollisionMark();
        jobSize = config.getMetricServiceConfiguration().getJobSize();
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

    /**
     * Partition metrics based on configured job size. Eagerly submit jobs to executor service
     * until a high collision is detected or all partitions are submitted.
     */
    @Override
    public Control push(Metric[] metrics) {
        if (metrics == null) {
            return Control.malformedRequest("metrics not nullable");
        }

        List<Metric> metricList = Arrays.asList(metrics);
        List< List<Metric>> metricPartitions = Lists.partition( metricList, jobSize);
        for ( List<Metric> partition : metricPartitions) {
            long totalInFlight = executorService.getTotalInFlight();
            if ( collides( totalInFlight, partition.size())) {
                return Control.dropped( "collision detected");
            }
            executorService.submit(partition);
        }

        return Control.ok();
    }

    public long getTotalInFlight() {
        return executorService.getTotalInFlight();
    }

    public long getTotalIncoming() {
        return executorService.getTotalIncoming();
    }

    public long getTotalOutGoing() {
        return executorService.getTotalOutGoing();
    }

    public long getTotalErrors() {
        return executorService.getTotalErrors();
    }

    /**
     * high/low collision test and increment, broad cast control messages
     */
    private boolean collides(long totalInFlight, int v) {
        //overflow...
        if (totalInFlight + v < 0) {
            eventBus.post(Control.highCollision());
            return true;
        }

        if (totalInFlight + v >= highCollisionMark) {
            eventBus.post(Control.highCollision());
            return true;
        }

        if (totalInFlight + v >= lowCollisionMark) {
            eventBus.post(Control.lowCollision());
        }

        return false;
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

    /** high collision detection mark */
    private int highCollisionMark;

    /** low collision detection mark*/
    private int lowCollisionMark;

    /** partion size of each job */
    private int jobSize;
}
