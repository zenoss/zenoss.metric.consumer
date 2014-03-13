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

import java.util.*;
import java.util.concurrent.*;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.AtomicLongMap;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.zenoss.app.consumer.metric.TsdbMetricsQueue;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.app.consumer.metric.remote.Utils;

/**
 * Threadsafe queue that can be used to distribute TSDB metric data to multiple
 * consumer threads.
 */
@Component
class MetricsQueue implements TsdbMetricsQueue {

    MetricsQueue() {
        this.queue = new LinkedBlockingQueue<>();
        this.perClientBacklog = AtomicLongMap.create();
        this.totalErrorsMetric = Metrics.newCounter(errorsMetricName());
        this.totalInFlightMetric = Metrics.newCounter(inFlightMetricName());
        this.totalIncomingMetric = registerIncoming();
        this.totalOutGoingMetric = registerOutgoing();
    }

    @Override
    public Collection<Metric> poll(int size, long maxWaitMillis) throws InterruptedException {
        Preconditions.checkArgument(size > 0);

        final Metric first = queue.poll(maxWaitMillis, TimeUnit.MILLISECONDS);
        if (first == null) {
            log.debug("Unable to retrieve a single element after max wait");
            return Collections.emptyList();
        }

        final Collection<Metric> metrics = new ArrayList<>(size);
        metrics.add(first);

        while (metrics.size() < size) {
            final Metric m = queue.poll();
            if (m == null) {
                log.debug("No more metrics in queue, retrieved {} metrics", metrics.size());
                break;
            }
            metrics.add(m);
        }

        for (final Multiset.Entry<String> e : clientCounts(metrics).entrySet()) {
            perClientBacklog.addAndGet(e.getElement(), - e.getCount());
        }
        perClientBacklog.removeAllZeros();
        return metrics;
    }

    /** Iterate over all the metrics' {@link #CLIENT_TAG} tag values, counting how many of each. */
    private Multiset<String> clientCounts(Collection<Metric> metrics) {
        Multiset<String> counts = HashMultiset.create();
        for (final Metric m : metrics) {
            String clientId = m.getTags().get(CLIENT_TAG);
            counts.add(clientId);
        }
        return counts;
    }

    @Override
    public void addAll(Collection<Metric> metrics, String clientId) {
        Utils.injectTag(TsdbMetricsQueue.CLIENT_TAG, clientId, metrics);
        queue.addAll(metrics);
        perClientBacklog.addAndGet(clientId, metrics.size());
        incrementIncoming(metrics.size());
    }

    @Override
    public void reAddAll(Collection<Metric> metrics) {
        Multiset<String> counts = clientCounts(metrics);
        queue.addAll(metrics);
        for (Multiset.Entry<String> e : counts.entrySet()) {
            perClientBacklog.addAndGet(e.getElement(), e.getCount());
        }
    }

    @Override
    public void incrementError(int size) {
        totalErrorsMetric.inc(size);
    }

    private void incrementIncoming(long incoming) {
        totalInFlightMetric.inc(incoming);
        totalIncomingMetric.mark(incoming);
    }

    @Override
    public void incrementProcessed(long processed) {
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

    @Override
    public long clientBacklogSize(String clientId) {
        return Math.max(0L, perClientBacklog.get(clientId));
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

    MetricName incomingMetricName() {
        return new MetricName(MetricsQueue.class, "totalIncoming");
    }

    MetricName outgoingMetricName() {
        return new MetricName(MetricsQueue.class, "totalOutgoing");
    }

    MetricName inFlightMetricName() {
        return new MetricName(MetricsQueue.class, "totalInFlight");
    }

    MetricName errorsMetricName() {
        return new MetricName(MetricsQueue.class, "totalErrors");
    }

    private static final Logger log = LoggerFactory.getLogger(MetricsQueue.class);

    /**
     * Data to be written to TSDB
     */
    private final BlockingQueue<Metric> queue;

    /**
     * Count of metrics in the queue, per client.
     * A metric's client is the value of its "remote_ip" tag.
     */
    // We use an AtomicLongMap instead of Multiset since it's theoretically possible for a count to dip below zero
    // due to thread interleaving, and we want to make sure that the increments and decrements balance out eventually.
    private final AtomicLongMap<String> perClientBacklog;

    /* ---------------------------------------------------------------------- *
     *  Yammer Metrics (internal to this process)                             *
     * ---------------------------------------------------------------------- */

    /**
     * How many errors occured writing to OpenTsdb
     */
    private final Counter totalErrorsMetric;

    /**
     * How many metrics are queued for processing
     */
    private final Counter totalInFlightMetric;

    /**
     * How many metrics were queued (this # may reset)
     */
    private Meter totalIncomingMetric;

    /**
     * How many metrics were written (this # may reset)
     */
    private Meter totalOutGoingMetric;
}
