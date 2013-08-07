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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;

@Component
class OpenTsdbMetricService implements MetricService {

    static final Logger log = LoggerFactory.getLogger(OpenTsdbMetricService.class);

    @Autowired
    OpenTsdbMetricService(
            MetricServiceConfiguration config, 
            @Qualifier("zapp::event-bus::async") EventBus eventBus, 
            MetricsQueue metricsQueue)
    {
        // Dependencies
        this.eventBus = eventBus;
        this.metricsQueue = metricsQueue;
        
        // Configuration
        this.highCollisionMark = config.getHighCollisionMark();
        this.lowCollisionMark = config.getLowCollisionMark();
        
        // State
        this.lastCollisionCount = new AtomicLong();
    }

    @Override
    public Control push(Metric[] metrics) {
        if (metrics == null) {
            return Control.malformedRequest("metrics not nullable");
        }
        
        long totalInFlight = metricsQueue.getTotalInFlight();
        if (collides (totalInFlight + metrics.length)) {
            return Control.dropped( "collision detected");
        }
        
        metricsQueue.addAll(Arrays.asList(metrics));
        
        // Notify the bus that we are going from no data to some data.
        if (totalInFlight == 0) {
            eventBus.post(Control.dataReceived());
            log.debug("Data received with zero metrics in flight");
        }
        
        return Control.ok();
    }

    /**
     * high/low collision test and increment, broad cast control messages
     */
    private boolean collides(long totalInFlight) {
        final long collisionCount = lastCollisionCount.getAndSet(totalInFlight);

        if (totalInFlight >= highCollisionMark) {
            eventBus.post(Control.highCollision());
            log.info("High collision: {}", totalInFlight);
            return true;
        }
        
        if (totalInFlight >= lowCollisionMark && totalInFlight > collisionCount) {
            eventBus.post(Control.lowCollision());
            log.debug("Low collision: {}", totalInFlight);
        }
        
        return false;
    }

    /** event bus for high/low collisions */
    private final EventBus eventBus;
    
    /** Shared data structure holding metrics to be pushed into TSDB  */
    private final MetricsQueue metricsQueue;

    /** high collision detection mark */
    private final int highCollisionMark;

    /** low collision detection mark*/
    private final int lowCollisionMark;
    
    /** 
     * Variable for tracking whether or not the number of collisions is
     * still going up or if it is going down
     */
    private final AtomicLong lastCollisionCount;

}
