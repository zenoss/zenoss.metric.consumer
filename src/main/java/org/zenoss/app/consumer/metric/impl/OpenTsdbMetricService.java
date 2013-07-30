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

import com.google.common.base.Preconditions;
import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.dropwizardspring.annotations.Managed;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.TsdbWriterFactory;

@Managed
public class OpenTsdbMetricService implements MetricService, com.yammer.dropwizard.lifecycle.Managed {

    static final Logger log = LoggerFactory.getLogger(OpenTsdbMetricService.class);

    @Autowired
    OpenTsdbMetricService(
            MetricServiceConfiguration config, 
            @Qualifier("zapp::event-bus::async") EventBus eventBus, 
            @Qualifier("zapp::executor::metrics") ExecutorService executorService,
            MetricsQueue metricsQueue,
            TsdbWriterFactory tsdbWriterFactory)
    {
        // Dependencies
        this.executorService = executorService;
        this.eventBus = eventBus;
        this.metricsQueue = metricsQueue;
        this.tsdbWriterFactory = tsdbWriterFactory;
        
        // Configuration
        this.highCollisionMark = config.getHighCollisionMark();
        this.lowCollisionMark = config.getLowCollisionMark();
        this.terminationTimeout = config.getTerminationTimeout();
        this.tsdbWriters = config.getTsdbWriterThreads();
    }

    @Override
    public void start() {
        log.info("start() [highCollisionMark: {}, lowCollisionMark: {}]", 
                highCollisionMark, lowCollisionMark);
        
        Preconditions.checkState(tsdbWriters > 0, 
                "TSDB writer threads must be > 0; check configuration.");
        
        for (int i=0; i < tsdbWriters; i++) {
            startWriter();
        }
    }

    @Override
    public void stop() throws InterruptedException {
        log.info("OpenTsdbMetricService.stop()");
        executorService.shutdownNow(); // Interrupt active threads
        executorService.awaitTermination(terminationTimeout, TimeUnit.SECONDS);
    }
    
    
    @Override
    public Future<?> startWriter() {
        return executorService.submit(tsdbWriterFactory.createWriter());
    }
    
    @Override
    public boolean stopWriter() {
        Iterator<TsdbWriter> iter = tsdbWriterFactory.getCreatedWriters().iterator();
        if (!iter.hasNext()) {
            log.info("Unable to stop writer because none exist");
            return false;
        }
        TsdbWriter writer = iter.next();
        iter.remove();
        writer.cancel(); // It may still take some amount of time to fully shutdown
        log.info("Writer canceled");
        return true;
    }

    /**
     * Eagerly submit metrics to the tail of the queue until a high collision 
     * is detected.
     */
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
        return Control.ok();
    }

    /**
     * high/low collision test and increment, broad cast control messages
     */
    private boolean collides(long totalInFlight) {
        if (totalInFlight >= highCollisionMark) {
            eventBus.post(Control.highCollision());
            lastCollisionCount = totalInFlight;
            log.info("High collision: {}", totalInFlight);
            return true;
        }

        if (totalInFlight >= lowCollisionMark && totalInFlight > lastCollisionCount) {
            eventBus.post(Control.lowCollision());
            log.debug("Low collision: {}", totalInFlight);
        }
        
        lastCollisionCount = totalInFlight;

        return false;
    }

    /** manage opentsd read/write jobs */
    private final ExecutorService executorService;

    /** event bus for high/low collisions */
    private final EventBus eventBus;
    
    /** Shared data structure holding metrics to be pushed into TSDB  */
    private final MetricsQueue metricsQueue;

    /** Factory for creating TsdbWriter instances */
    private final TsdbWriterFactory tsdbWriterFactory;

    /** high collision detection mark */
    private final int highCollisionMark;

    /** low collision detection mark*/
    private final int lowCollisionMark;
    
    /** How long should we wait for the executor service to shutdown? */
    private final int terminationTimeout;
    
    /** How many TSDB writer threads should we start? */
    private final int tsdbWriters;
    
    /** 
     * Variable for tracking whether or not the number of collisions is
     * still going up or if it is going down
     */
    private long lastCollisionCount = 0;

}
