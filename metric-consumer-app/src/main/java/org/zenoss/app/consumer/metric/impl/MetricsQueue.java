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
import com.codahale.metrics.*;
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
    protected MetricRegistry metricRegistry;

    private final static Supplier<Boolean> YEPYEP = new Supplier<Boolean>() {
        @Override public Boolean get() {return true;}
    };

    MetricsQueue() {
        this(new MetricRegistry());
    }

    MetricsQueue(MetricRegistry registry) {
        this.metricRegistry = registry;
        this.queue = new LinkedBlockingQueue<>();
        this.perClientBacklog = AtomicLongMap.create();
        this.totalErrorsMetric = registerTotalErrors();
        this.totalInFlightMetric = registerTotalInFlight();
        this.totalClientCountMetric = metricRegistry.register(clientCountMetricName(),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
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
        log.debug("AddAll exit. queue.size() = {}", queue.size());
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

    private Counter registerTotalErrors() {return metricRegistry.counter(errorsMetricName());}

    private Counter registerTotalInFlight() {return metricRegistry.counter(inFlightMetricName());}

    private Meter registerIncoming() {
        return metricRegistry.meter(incomingMetricName());
    }

    private Meter registerOutgoing() {
        return metricRegistry.meter(outgoingMetricName());
    }

    private Meter registerLost() {
        return metricRegistry.meter(lostMetricName());
    }

    private Meter registerReceived() {
        return metricRegistry.meter(receivedMetricName());
    }

    private Meter registerRejected() {
        return metricRegistry.meter(rejectedMetricName());
    }

    private Meter registerHighCollision() {
        return metricRegistry.meter(highCollisionMetricName());
    }

    private Meter registerLowCollision() {
        return metricRegistry.meter(lowCollisionMetricName());
    }

    private Meter registerClientCollision() {
        return metricRegistry.meter(clientCollisionMetricName());
    }

    private Meter registerBroadcastHighCollision() {
        return metricRegistry.meter(broadcastHighCollisionMetricName());
    }

    private Meter registerBroadcastLowCollision() {
        return metricRegistry.meter(broadcastLowCollisionMetricName());
    }

    private Meter registerSentClientCollision() {
        return metricRegistry.meter(sentClientCollisionMetricName());
    }

    // Used for testing
    void resetMetrics() {
        MetricRegistry registry = metricRegistry;
        registry.remove(errorsMetricName());
        registry.remove(inFlightMetricName());
        registry.remove(incomingMetricName());
        registry.remove(outgoingMetricName());
        registry.remove(lostMetricName());
        registry.remove(receivedMetricName());
        registry.remove(highCollisionMetricName());
        registry.remove(lowCollisionMetricName());
        registry.remove(clientCollisionMetricName());
        registry.remove(broadcastHighCollisionMetricName());
        registry.remove(broadcastLowCollisionMetricName());
        registry.remove(sentClientCollisionMetricName());
        totalErrorsMetric = registerTotalErrors();
        totalInFlightMetric = registerTotalInFlight();
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
        return totalInFlightMetric.getCount();
    }

    @Override
    public long clientBacklogSize(String clientId) {
        return Math.max(0L, perClientBacklog.get(clientId));
    }

    long getTotalErrors() {
        return totalErrorsMetric.getCount();
    }

    long getTotalIncoming() {
        return totalIncomingMetric.getCount();
    }

    long getTotalOutgoing() {
        return totalOutGoingMetric.getCount();
    }

    long getTotalReceived() {
        return totalReceivedMetric.getCount();
    }

    long getTotalRejected() {
        return totalRejectedMetric.getCount();
    }

    long getTotalLost() {
        return totalLostMetric.getCount();
    }

    double getOneMinuteIncoming() {
        return totalIncomingMetric.getOneMinuteRate();
    }

    double getOneMinuteOutgoing() {
        return totalOutGoingMetric.getOneMinuteRate();
    }

    double getOneMinuteReceived() {
        return totalReceivedMetric.getOneMinuteRate();
    }

    double getOneMinuteRejected() {
        return totalRejectedMetric.getOneMinuteRate();
    }

    double getOneMinuteLost() {
        return totalLostMetric.getOneMinuteRate();
    }

    double getOneMinuteHighCollision() {
        return totalHighCollisionMetric.getOneMinuteRate();
    }

    double getOneMinuteLowCollision() {
        return totalLowCollisionMetric.getOneMinuteRate();
    }

    double getOneMinuteClientCollision() {
        return totalClientCollisionMetric.getOneMinuteRate();
    }

    double getOneMinuteBroadcastHighCollision() {
        return totalBroadcastHighCollisionMetric.getOneMinuteRate();
    }

    double getOneMinuteBroadcastLowCollision() {
        return totalBroadcastLowCollisionMetric.getOneMinuteRate();
    }

    double getOneMinuteSentClientCollision() {
        return totalSentClientCollisionMetric.getOneMinuteRate();
    }

    String incomingMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalIncoming");
    }

    String outgoingMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalOutgoing");
    }

    String inFlightMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalInFlight");
    }

    String clientCountMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalClientCount");
    }

    String errorsMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalErrors");
    }

    String lostMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalLost");
    }

    String receivedMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalReceived");
    }

    String rejectedMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalRejected");
    }

    String highCollisionMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalHighCollision");
    }

    String lowCollisionMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalLowCollision");
    }

    String clientCollisionMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalClientCollision");
    }

    String broadcastHighCollisionMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalBroadcastHighCollision");
    }

    String broadcastLowCollisionMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalBroadcastLowCollision");
    }

    String sentClientCollisionMetricName() {
        return MetricRegistry.name(MetricsQueue.class, "totalSentClientCollision");
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
    private Counter totalErrorsMetric;

    /**
     * How many metrics are queued for processing
     */
    private Counter totalInFlightMetric;

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
