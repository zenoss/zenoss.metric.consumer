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
        this.highCollisionMark = config.getMetricServiceConfiguration().getHighCollisionMark();
        this.lowCollisionMark = config.getMetricServiceConfiguration().getLowCollisionMark();
        this.jobSize = config.getMetricServiceConfiguration().getJobSize();
        this.minTimeBetweenBroadcast = config.getMetricServiceConfiguration().getMinTimeBetweenBroadcast();
    }

    @Override
    public void start() {
        log.info( "start() [highCollisionMark: {}, lowCollisionMark: {}, jobSize: {}]", 
                highCollisionMark, lowCollisionMark, jobSize);
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
        final long totalPlusV = totalInFlight + v;

        if (totalPlusV >= highCollisionMark) {
            if (System.currentTimeMillis() > lastHighCollisionEvent + minTimeBetweenBroadcast) {
                log.warn("High collision threshold exceeded: {}", totalPlusV);
                lastHighCollisionEvent = System.currentTimeMillis();
                eventBus.post(Control.highCollision());
                lastCollisionCount = totalPlusV;
            }
            return true;
        }

        if (totalPlusV >= lowCollisionMark && 
                System.currentTimeMillis() > lastLowCollisionEvent + minTimeBetweenBroadcast &&
                    totalPlusV > lastCollisionCount) 
        {
            log.info("Low collision threshold exceeded: {}", totalPlusV);
            lastLowCollisionEvent = System.currentTimeMillis();
            eventBus.post(Control.lowCollision());
        }
        
        lastCollisionCount = totalPlusV;

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
    
    private long lastHighCollisionEvent = 0;
    private long lastLowCollisionEvent = 0;
    private long lastCollisionCount = 0;
    private long minTimeBetweenBroadcast = 500;

    /** partion size of each job */
    private int jobSize;
}
