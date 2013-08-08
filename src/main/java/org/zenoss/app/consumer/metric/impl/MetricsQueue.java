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

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.zenoss.app.consumer.metric.TsdbMetricsQueue;
import org.zenoss.app.consumer.metric.data.Metric;

/**
 * Threadsafe queue that can be used to distribute TSDB metric data to multiple 
 * consumer threads.
 */
@Component
class MetricsQueue implements TsdbMetricsQueue {
        
    MetricsQueue() {
        this.queue = new ConcurrentLinkedQueue<>();
        this.totalErrorsMetric = Metrics.newCounter(errorsMetricName());
        this.totalInFlightMetric = Metrics.newCounter(inFlightMetricName());
        this.totalIncomingMetric = registerIncoming();
        this.totalOutGoingMetric = registerOutgoing();
    }
    
    @Override
    public Collection<Metric> poll(int size) {
        final Collection<Metric> metrics = new ArrayList<>(size);
        while(metrics.size() < size) {
            final Metric m = queue.poll();
            if (m == null) {
                log.debug("Unable to retrieve metric from queue");
                break;
            }
            metrics.add(m);
        }
        return metrics;
    }
    
    @Override
    public void addAll(Collection<Metric> metrics) {
        addAll(metrics, false);
    }
    
    void addAll(Collection<Metric> metrics, boolean alreadyCounted) {
        queue.addAll(metrics);
        if (!alreadyCounted) {
            incrementIncoming(metrics.size(), metrics.size());
        }
    }
    
    public void incrementError(int size) {
        totalErrorsMetric.inc(size);
    }

    private void incrementIncoming(long incomingSize, long addedSize) {
        totalInFlightMetric.inc(addedSize);
        totalIncomingMetric.mark(incomingSize);
    }

    void incrementProcessed(long processed) {
        totalInFlightMetric.dec(processed);
        totalOutGoingMetric.mark(processed);
    }
    
    private Meter registerIncoming() {
        return Metrics.newMeter(incomingMetricName(), "metrics", TimeUnit.SECONDS);
    }
    
    private Meter registerOutgoing() {
        return Metrics.newMeter(outgoingMetricName(), "metrics", TimeUnit.SECONDS);
    }
    
    // Used for testing
    void resetMetrics() {
        totalErrorsMetric.clear();
        totalInFlightMetric.clear();
        MetricsRegistry registry = Metrics.defaultRegistry();
        registry.removeMetric(incomingMetricName());
        registry.removeMetric(outgoingMetricName());
        totalIncomingMetric = registerIncoming();
        totalOutGoingMetric = registerOutgoing();
    }

    @Override
    public long getTotalInFlight() {
        return totalInFlightMetric.count();
    }
    
    long getTotalErrors() {
        return totalErrorsMetric.count();
    }
    
    long getTotalIncoming() {
        return totalIncomingMetric.count();
    }
    
    long getTotalOutgoing() {
        return totalOutGoingMetric.count();
    }
    
    double getOneMinuteIncoming() {
        return totalIncomingMetric.oneMinuteRate();
    }
    
    double getOneMinuteOutgoing() {
        return totalOutGoingMetric.oneMinuteRate();
    }
    
    private MetricName incomingMetricName() {
        return new MetricName(MetricsQueue.class, "totalIncoming");
    }

    private MetricName outgoingMetricName() {
        return new MetricName(MetricsQueue.class, "totalOutgoing");
    }
    
    private MetricName inFlightMetricName() {
        return new MetricName(MetricsQueue.class, "totalInFlight");
    }

    private MetricName errorsMetricName() {
        return new MetricName(MetricsQueue.class, "totalErrors");
    }
    
    private static final Logger log = LoggerFactory.getLogger(MetricsQueue.class);
    
    /** Data to be written to TSDB */
    private final Queue<Metric> queue;
        
    /* ---------------------------------------------------------------------- *
     *  Yammer Metrics (internal to this process)                             *
     * ---------------------------------------------------------------------- */
    
    /** How many errors occured writing to OpenTsdb */
    private final Counter totalErrorsMetric;
    
    /** How many metrics are queued for processing */
    private final Counter totalInFlightMetric;
    
    /** How many metrics were queued (this # may reset) */
    private Meter totalIncomingMetric;
    
    /** How many metrics were written (this # may reset) */
    private Meter totalOutGoingMetric;
}
