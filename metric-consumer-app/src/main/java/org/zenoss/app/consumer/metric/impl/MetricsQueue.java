/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */
package org.zenoss.app.consumer.metric.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.AtomicLongMap;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.zenoss.app.consumer.metric.TsdbMetricsQueue;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.app.consumer.metric.remote.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Threadsafe queue that can be used to distribute TSDB metric data to multiple
 * consumer threads.
 */
@Component
class MetricsQueue implements TsdbMetricsQueue {

    private final static Supplier<Boolean> YEPYEP = new Supplier<Boolean>() {
        @Override public Boolean get() {return true;}
    };

    MetricsQueue() {
        this.queue = new LinkedBlockingQueue<>();
        this.perClientBacklog = AtomicLongMap.create();
        this.totalErrorsMetric = Metrics.newCounter(errorsMetricName());
        this.totalInFlightMetric = Metrics.newCounter(inFlightMetricName());
        this.totalClientCountMetric = Metrics.newGauge(clientCountMetricName(), new Gauge<Long>() {
            @Override
            public Long value() {
                return clientCount();
            }
        });
        this.totalIncomingMetric = registerIncoming();
        this.totalOutGoingMetric = registerOutgoing();
        this.totalReceivedMetric = registerReceived();
        this.totalRejectedMetric = registerRejected();
        this.totalLostMetric = registerLost();
        this.totalHighCollisionMetric = registerHighCollision();
        this.totalLowCollisionMetric = registerLowCollision();
        this.totalClientCollisionMetric = registerClientCollision();
        this.totalBroadcastHighCollisionMetric = registerBroadcastHighCollision();
        this.totalBroadcastLowCollisionMetric = registerBroadcastLowCollision();
        this.totalSentClientCollisionMetric = registerSentClientCollision();
        this.recentClientIds = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.SECONDS).build(CacheLoader.from(YEPYEP));
    }

    @Override
    public Collection<Metric> poll(int size, long maxWaitMillis) throws InterruptedException {
        Preconditions.checkArgument(size > 0);

        log.debug("Polling. size = {}, queue size = {}", size, queue.size());
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

    @Override
    public long clientCount() {
        perClientBacklog.removeAllZeros();
        return Math.max(perClientBacklog.size(), recentClientIds.size());
    }


    /** Iterate over all the metrics' {@link #CLIENT_TAG} tag values, counting how many of each. */
    private Multiset<String> clientCounts(Collection<Metric> metrics) {
        Multiset<String> counts = HashMultiset.create();
        for (final Metric m : metrics) {
            String clientId = m.getTags().get(CLIENT_TAG);
            if (clientId == null) {
                log.error("Metric {} missing required tag {}. throwing IllegalStateException", m.toString(), CLIENT_TAG);
                throw new IllegalStateException("Metric missing required tag: " + CLIENT_TAG);
            }
            counts.add(clientId);
        }
        return counts;
    }

    @Override
    public void addAll(Collection<Metric> metrics, String clientId) {
        if (null == queue) {
            log.warn("queue is null. Nothing will be added.");
            return;
        }
        log.debug("AddAll entry. clientId = {}, queue.size() = {}", clientId, queue.size());
        Utils.injectTag(TsdbMetricsQueue.CLIENT_TAG, clientId, metrics);
        queue.addAll(metrics);
        perClientBacklog.addAndGet(clientId, metrics.size());
        recentClientIds.getUnchecked(clientId);
        incrementIncoming(metrics.size());
        log.debug("AddAll exit. clientId = {}, queue.size() = {}", clientId, queue.size());
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

    @Override
    public void incrementLostMetrics(long lost) {
        totalInFlightMetric.dec(lost);
        totalLostMetric.mark(lost);
    }

    @Override
    public void incrementReceived(long received) {
        totalReceivedMetric.mark(received);
    }

    @Override
    public void incrementRejected(long rejected) {
        totalRejectedMetric.mark(rejected);
    }

    @Override
    public void incrementHighCollision() {
        totalHighCollisionMetric.mark();
    }

    @Override
    public void incrementLowCollision() {
        totalLowCollisionMetric.mark();
    }

    @Override
    public void incrementClientCollision() {
        totalClientCollisionMetric.mark();
    }

    @Override
    public void incrementSentClientCollision() {
        totalSentClientCollisionMetric.mark();
    }

    @Override
    public void incrementBroadcastLowCollision() {
        totalBroadcastLowCollisionMetric.mark();
    }

    @Override
    public void incrementBroadcastHighCollision() {
        totalBroadcastHighCollisionMetric.mark();
    }


    private Meter registerIncoming() {
        return Metrics.newMeter(incomingMetricName(), "metrics", TimeUnit.SECONDS);
    }

    private Meter registerOutgoing() {
        return Metrics.newMeter(outgoingMetricName(), "metrics", TimeUnit.SECONDS);
    }

    private Meter registerLost() {
        return Metrics.newMeter(lostMetricName(), "metrics", TimeUnit.SECONDS);
    }

    private Meter registerReceived() {
        return Metrics.newMeter(receivedMetricName(), "metrics", TimeUnit.SECONDS);
    }

    private Meter registerRejected() {
        return Metrics.newMeter(rejectedMetricName(), "metrics", TimeUnit.SECONDS);
    }

    private Meter registerHighCollision() {
        return Metrics.newMeter(highCollisionMetricName(), "metrics", TimeUnit.SECONDS);
    }

    private Meter registerLowCollision() {
        return Metrics.newMeter(lowCollisionMetricName(), "metrics", TimeUnit.SECONDS);
    }

    private Meter registerClientCollision() {
        return Metrics.newMeter(clientCollisionMetricName(), "metrics", TimeUnit.SECONDS);
    }

    private Meter registerBroadcastHighCollision() {
        return Metrics.newMeter(broadcastHighCollisionMetricName(), "metrics", TimeUnit.SECONDS);
    }

    private Meter registerBroadcastLowCollision() {
        return Metrics.newMeter(broadcastLowCollisionMetricName(), "metrics", TimeUnit.SECONDS);
    }

    private Meter registerSentClientCollision() {
        return Metrics.newMeter(sentClientCollisionMetricName(), "metrics", TimeUnit.SECONDS);
    }

    // Used for testing
    void resetMetrics() {
        totalErrorsMetric.clear();
        totalInFlightMetric.clear();
        MetricsRegistry registry = Metrics.defaultRegistry();
        registry.removeMetric(incomingMetricName());
        registry.removeMetric(outgoingMetricName());
        registry.removeMetric(lostMetricName());
        registry.removeMetric(receivedMetricName());
        registry.removeMetric(highCollisionMetricName());
        registry.removeMetric(lowCollisionMetricName());
        registry.removeMetric(clientCollisionMetricName());
        registry.removeMetric(broadcastHighCollisionMetricName());
        registry.removeMetric(broadcastLowCollisionMetricName());
        registry.removeMetric(sentClientCollisionMetricName());
        totalIncomingMetric = registerIncoming();
        totalOutGoingMetric = registerOutgoing();
        totalLostMetric = registerLost();
        totalHighCollisionMetric = registerHighCollision();
        totalLowCollisionMetric = registerLowCollision();
        totalClientCollisionMetric = registerClientCollision();
        totalBroadcastHighCollisionMetric = registerBroadcastHighCollision();
        totalBroadcastLowCollisionMetric = registerBroadcastLowCollision();
        totalSentClientCollisionMetric = registerSentClientCollision();
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

    long getTotalReceived() {
        return totalReceivedMetric.count();
    }

    long getTotalRejected() {
        return totalRejectedMetric.count();
    }

    long getTotalLost() {
        return totalLostMetric.count();
    }

    double getOneMinuteIncoming() {
        return totalIncomingMetric.oneMinuteRate();
    }

    double getOneMinuteOutgoing() {
        return totalOutGoingMetric.oneMinuteRate();
    }

    double getOneMinuteReceived() {
        return totalReceivedMetric.oneMinuteRate();
    }

    double getOneMinuteRejected() {
        return totalRejectedMetric.oneMinuteRate();
    }

    double getOneMinuteLost() {
        return totalLostMetric.oneMinuteRate();
    }

    double getOneMinuteHighCollision() {
        return totalHighCollisionMetric.oneMinuteRate();
    }

    double getOneMinuteLowCollision() {
        return totalLowCollisionMetric.oneMinuteRate();
    }

    double getOneMinuteClientCollision() {
        return totalClientCollisionMetric.oneMinuteRate();
    }

    double getOneMinuteBroadcastHighCollision() {
        return totalBroadcastHighCollisionMetric.oneMinuteRate();
    }

    double getOneMinuteBroadcastLowCollision() {
        return totalBroadcastLowCollisionMetric.oneMinuteRate();
    }

    double getOneMinuteSentClientCollision() {
        return totalSentClientCollisionMetric.oneMinuteRate();
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

    MetricName clientCountMetricName() {
        return new MetricName(MetricsQueue.class, "totalClientCount");
    }

    MetricName errorsMetricName() {
        return new MetricName(MetricsQueue.class, "totalErrors");
    }

    MetricName lostMetricName() {
        return new MetricName(MetricsQueue.class, "totalLost");
    }

    MetricName receivedMetricName() {
        return new MetricName(MetricsQueue.class, "totalReceived");
    }

    MetricName rejectedMetricName() {
        return new MetricName(MetricsQueue.class, "totalRejected");
    }

    MetricName highCollisionMetricName() {
        return new MetricName(MetricsQueue.class, "totalHighCollision");
    }

    MetricName lowCollisionMetricName() {
        return new MetricName(MetricsQueue.class, "totalLowCollision");
    }

    MetricName clientCollisionMetricName() {
        return new MetricName(MetricsQueue.class, "totalClientCollision");
    }

    MetricName broadcastHighCollisionMetricName() {
        return new MetricName(MetricsQueue.class, "totalBroadcastHighCollision");
    }

    MetricName broadcastLowCollisionMetricName() {
        return new MetricName(MetricsQueue.class, "totalBroadcastLowCollision");
    }

    MetricName sentClientCollisionMetricName() {
        return new MetricName(MetricsQueue.class, "totalSentClientCollision");
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

    /**
     * Client IDs that have been seen recently.
     */
    private final LoadingCache<String,Boolean> recentClientIds;

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
     * How many unique clients have we seen recently and/or currently have metrics in the queue?
     */
    private final Gauge totalClientCountMetric;

    /**
     * How many metrics were queued (this # may reset)
     */
    private Meter totalIncomingMetric;

    /**
     * How many metrics were written (this # may reset)
     */
    private Meter totalOutGoingMetric;

    /**
     * How many metrics were lost (this # may reset)
     */
    private Meter totalLostMetric;

    /**
     * How many metrics were received (not necessarily accepted)
     */
    private Meter totalReceivedMetric;

    /**
     * How many metrics were received but rejected
     */
    private Meter totalRejectedMetric;

    /**
     * How many high collision events?
     */
    private Meter totalHighCollisionMetric;

    /**
     * How many low collision events?
     */
    private Meter totalLowCollisionMetric;

    /**
     * How many client collision events?
     */
    private Meter totalClientCollisionMetric;

    /**
     * How many high collision events were broadcast?
     */
    private Meter totalBroadcastHighCollisionMetric;

    /**
     * How many low collision events were broadcast?
     */
    private Meter totalBroadcastLowCollisionMetric;

    /**
     * How many client collision events were sent via websocket?
     */
    private Meter totalSentClientCollisionMetric;

}
