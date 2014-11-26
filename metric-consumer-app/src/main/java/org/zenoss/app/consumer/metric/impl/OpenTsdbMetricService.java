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

import com.google.api.client.util.ExponentialBackOff;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Component
class OpenTsdbMetricService implements MetricService {

    private static final Logger log = LoggerFactory.getLogger(OpenTsdbMetricService.class);

    @Autowired
    OpenTsdbMetricService(
            MetricServiceConfiguration config,
            @Qualifier("zapp::event-bus::async") EventBus eventBus,
            MetricsQueue metricsQueue) {
        // Dependencies
        this.eventBus = eventBus;
        this.metricsQueue = metricsQueue;

        // Configuration
        this.highCollisionMark = config.getHighCollisionMark();
        this.lowCollisionMark = config.getLowCollisionMark();
        this.perClientMaxBacklogSize = config.getPerClientMaxBacklogSize();
        this.maxClientWaitTime = config.getMaxClientWaitTime();
        this.maxPushSize = Math.max(highCollisionMark - 1, perClientMaxBacklogSize);

        // State
        this.lastCollisionCount = new AtomicLong();
    }

    @Override
    public Control push(final List<Metric> metrics, final String clientId) {
        if (metrics == null) {
            return Control.malformedRequest("metrics not nullable");
        }
        if (clientId == null) {
            return Control.malformedRequest("clientId not nullable");
        }
        if (metrics.size() > maxPushSize) {
            String reason = String.format("cannot push more than %d metrics at a time", maxPushSize);
            return Control.malformedRequest(reason);
        }
        final List<Metric> copy = Lists.newArrayList(metrics);
        if (!copy.isEmpty()) {
            long totalInFlight = metricsQueue.getTotalInFlight();
            log.debug("totalInFlight = {}", totalInFlight);
            if (keepsColliding(copy.size(), clientId)) {
                return Control.dropped("collision detected");
            }

            metricsQueue.addAll(copy, clientId);

            // Notify the bus that we are going from no data to some data.
            if (totalInFlight == 0) {
                eventBus.post(Control.dataReceived());
                log.debug("Data received with zero metrics in flight");
            } else {
                log.debug("totalInFlight is nonzero. Sending dataReceived event anyway.");
                // ZEN-11665: In the event of an openTSDB shutdown, we could be left with inFlight metrics, but not have an event triggered.
                //            Post event for nonzero inFlight so queue doesn't stop polling.
                eventBus.post(Control.dataReceived());
            }
        }

        return Control.ok();

    }

    /**
     * Checks {@link #collides(long, String)} until it returns false, or we give up.
     * Periodic checks are spaced out using an exponential back off.
     * @param incomingSize the number of metrics being added
     * @param clientId an identifier for the source of the metrics
     * @return false if and only if {@link #collides(long, String)} returned false before we gave up.
     */
    private boolean keepsColliding(final long incomingSize, final String clientId) {
        ExponentialBackOff backOffTracker = null;
        int collisions = 0;
        while (collides(incomingSize, clientId)) {
            collisions++;
            if (backOffTracker == null) {
                backOffTracker = buildExponentialBackOff();
            }
            long backOff;
            try {
                backOff = backOffTracker.nextBackOffMillis();
            } catch (IOException e) {
                // should never happen
                log.error("Caught IOException backing off tracker.", e);
                throw new RuntimeException(e);
            }
            long elapsed = backOffTracker.getElapsedTimeMillis();
            if (ExponentialBackOff.STOP == backOff) {
                log.warn("Too many collisions ({}). Gave up after {}ms.", collisions, elapsed);
                return true;
            } else {
                log.debug("Collision detected ({} in {}ms). Backing off for {} ms", collisions, elapsed, backOff);
                try {
                    Thread.sleep(backOff);
                } catch (InterruptedException e) { /* no biggie */ }
            }
        }
        return false;
    }

    private ExponentialBackOff buildExponentialBackOff() {
        return new ExponentialBackOff.Builder().
            setMaxElapsedTimeMillis(maxClientWaitTime).
            build();
    }

    /**
     * high/low collision test and increment, broad cast control messages
     */
    private boolean collides(final long incomingSize, final String clientId) {
        long totalInFlight = metricsQueue.getTotalInFlight() + incomingSize;
        final long collisionCount = lastCollisionCount.getAndSet(totalInFlight);

        if (totalInFlight >= highCollisionMark) {
            eventBus.post(Control.highCollision());
            log.info("High collision: {}", totalInFlight);
            return true;
        }

        long clientBacklogSize = metricsQueue.clientBacklogSize(clientId);
        if (totalInFlight >= lowCollisionMark) {
            if (totalInFlight > collisionCount) {
                eventBus.post(Control.lowCollision());
                log.debug("Low collision: {}", totalInFlight);
            }
            if (clientBacklogSize > 0)
                return true;
        } else if (clientBacklogSize + incomingSize > perClientMaxBacklogSize) {
            log.debug("Client's max backlog size ({}) exceeded.", perClientMaxBacklogSize);
            return true;
        }

        return false;
    }

    /**
     * event bus for high/low collisions
     */
    private final EventBus eventBus;

    /**
     * Shared data structure holding metrics to be pushed into TSDB
     */
    private final MetricsQueue metricsQueue;

    /**
     * high collision detection mark
     */
    private final int highCollisionMark;

    /**
     * low collision detection mark
     */
    private final int lowCollisionMark;

    /**
     * per-client maximum backlog size
     */
    private final int perClientMaxBacklogSize;

    /**
     * maximum time for a client to wait to add metrics to a backlogged queue before giving up.
     */
    private final int maxClientWaitTime;

    /**
     * maximum count of metrics that can be pushed per call to {@link #push(java.util.List, String)}
     */
    private final int maxPushSize;

    /**
     * Variable for tracking whether or not the number of collisions is
     * still going up or if it is going down
     */
    private final AtomicLong lastCollisionCount;

}
