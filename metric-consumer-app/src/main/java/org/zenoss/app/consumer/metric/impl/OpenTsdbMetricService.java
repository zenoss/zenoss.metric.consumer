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
import org.zenoss.app.consumer.metric.TsdbMetricsQueue;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Component
class OpenTsdbMetricService implements MetricService {

	private static final Logger log = LoggerFactory.getLogger(OpenTsdbMetricService.class);

	@Autowired
	OpenTsdbMetricService(MetricServiceConfiguration config, @Qualifier("zapp::event-bus::async") EventBus eventBus,
			MetricsQueue metricsQueue, DatabusMetricsQueue databusMetricsQueue) {
		// Dependencies
		this.eventBus = eventBus;
		this.metricsQueue = metricsQueue;
		this.databusMetricsQueue = databusMetricsQueue;
		this.enableDatabusPublish = config.getDatabusPublishConfig().isEnablePublish();
		// Configuration
		this.highCollisionMark = config.getHighCollisionMark();
		this.lowCollisionMark = config.getLowCollisionMark();
		this.perClientMaxBacklogSize = config.getPerClientMaxBacklogSize();
		this.perClientMaxPercentOfFairBacklogSize = config.getPerClientMaxPercentOfFairBacklogSize();
		this.maxClientWaitTime = config.getMaxClientWaitTime();
		this.minTimeBetweenRetries = config.getMinTimeBetweenNotification();

		// State
		this.lastCollisionCount = new AtomicLong();
	}

	@Override
	public void incrementReceived(long received) {
		metricsQueue.incrementReceived(received);
	}

	@Override
	public void incrementSentClientCollision() {
		metricsQueue.incrementSentClientCollision();
	}

	@Override
	public void incrementBroadcastLowCollision() {
		metricsQueue.incrementBroadcastLowCollision();
	}

	@Override
	public void incrementBroadcastHighCollision() {
		metricsQueue.incrementBroadcastHighCollision();
	}

	@Override
	public Control push(final List<Metric> metrics, final String clientId, Runnable onCollision) {
		if (metrics == null) {
			return Control.malformedRequest("metrics not nullable");
		}
		if (clientId == null) {
			metricsQueue.incrementRejected(metrics.size());
			log.info("Rejected: [{}] clientId not nullable", metrics.size());
			return Control.malformedRequest("clientId not nullable");
		}
		long maxPushSize = Math.max(highCollisionMark - 1, perClientMaxBacklogSize());
		if (metrics.size() > maxPushSize) {
			String reason = String.format("cannot push more than %d metrics at a time", maxPushSize);
			metricsQueue.incrementRejected(metrics.size());
			log.info("Rejected: [{}] {}", metrics.size(), reason);
			return Control.malformedRequest(reason);
		}
		final List<Metric> copy = Lists.newArrayList(metrics);

		if (!copy.isEmpty()) {
			long totalInFlight = metricsQueue.getTotalInFlight();
			log.debug("totalInFlight = {}", totalInFlight);
			if (keepsColliding(copy.size(), clientId, onCollision)) {
				log.info("Rejected: [{}] consumer is overwhelmed", metrics.size());
				metricsQueue.incrementRejected(metrics.size());
				return Control.dropped("consumer is overwhelmed");
			}

			metricsQueue.addAll(copy, clientId);

			if (this.enableDatabusPublish) {
				final List<Metric> copy2 = Lists.newArrayList(metrics);
				databusMetricsQueue.addAll(copy2);
			}
			// Notify the bus that we are going from no data to some data.
			if (totalInFlight == 0) {
				eventBus.post(Control.dataReceived());
				log.debug("Data received with zero metrics in flight");
			} else {
				log.debug("totalInFlight is nonzero. Sending dataReceived event anyway.");
				// ZEN-11665: In the event of an openTSDB shutdown, we could be
				// left with inFlight metrics, but not have an event triggered.
				// Post event for nonzero inFlight so queue doesn't stop
				// polling.
				eventBus.post(Control.dataReceived());
			}
		}

		return Control.ok();

	}

	/**
	 * Checks {@link #collides(long, String)} until it returns false, or we give
	 * up. Periodic checks are spaced out using an exponential back off.
	 * 
	 * @param incomingSize
	 *            the number of metrics being added
	 * @param clientId
	 *            an identifier for the source of the metrics
	 * @return false if and only if {@link #collides(long, String)} returned
	 *         false before we gave up.
	 */
	private boolean keepsColliding(final long incomingSize, final String clientId, Runnable onCollision) {
		ExponentialBackOff backOffTracker = null;
		long retryAt = 0L;
		long backOff;
		int collisions = 0;
		while (true) {
			if (collisions > 0 && onCollision != null)
				onCollision.run();
			if (System.currentTimeMillis() > retryAt) {
				if (collides(incomingSize, clientId)) {
					metricsQueue.incrementClientCollision();
					collisions++;
					if (backOffTracker == null) {
						backOffTracker = buildExponentialBackOff();
					}
					try {
						backOff = backOffTracker.nextBackOffMillis();
						retryAt = System.currentTimeMillis() + backOff;
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
						log.debug("Collision detected ({} in {}ms). Backing off for {} ms", collisions, elapsed,
								backOff);
					}
				} else {
					return false;
				}
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				/* no biggie */ }
		}
	}

	private ExponentialBackOff buildExponentialBackOff() {
		return new ExponentialBackOff.Builder().setInitialIntervalMillis(1).setMaxIntervalMillis(minTimeBetweenRetries)
				.setMaxElapsedTimeMillis(maxClientWaitTime).build();
	}

	/**
	 * high/low collision test and increment, broad cast control messages
	 */
	private boolean collides(final long incomingSize, final String clientId) {
		long totalInFlight = metricsQueue.getTotalInFlight() + incomingSize;
		final long collisionCount = lastCollisionCount.getAndSet(totalInFlight);
		long perClientMaxBacklogSize = perClientMaxBacklogSize();
		if (totalInFlight >= highCollisionMark) {
			eventBus.post(Control.highCollision());
			log.info("High collision: {}", totalInFlight);
			metricsQueue.incrementHighCollision();
			return true;
		}

		long clientBacklogSize = metricsQueue.clientBacklogSize(clientId);
		if (totalInFlight >= lowCollisionMark) {
			if (totalInFlight > collisionCount) {
				eventBus.post(Control.lowCollision());
				log.debug("Low collision: {}", totalInFlight);
				metricsQueue.incrementLowCollision();
			}
			if (clientBacklogSize > 0)
				return true;
		} else if (clientBacklogSize + incomingSize > perClientMaxBacklogSize) {
			log.debug("Client's max backlog size ({}) exceeded.", perClientMaxBacklogSize);
			return true;
		}

		return false;
	}

	private long perClientMaxBacklogSize() {
		if (this.perClientMaxBacklogSize > 0)
			return this.perClientMaxBacklogSize;
		long clientCount = Math.max(1, metricsQueue.clientCount());
		long result = perClientMaxPercentOfFairBacklogSize * lowCollisionMark / clientCount / 100;
		if (result > highCollisionMark - 1)
			return highCollisionMark - 1;
		if (result < 64)
			return 64;
		return result;
	}

	/**
	 * event bus for high/low collisions
	 */
	private final EventBus eventBus;

	/**
	 * Shared data structure holding metrics to be pushed into TSDB
	 */
	private final TsdbMetricsQueue metricsQueue;

	private final DatabusMetricsQueue databusMetricsQueue;

	private final boolean enableDatabusPublish;
	/**
	 * high collision detection mark
	 */
	private final int highCollisionMark;

	/**
	 * low collision detection mark
	 */
	private final int lowCollisionMark;

	/**
	 * Each client should be allowed to backlog up to global max backlog
	 * (lowCollisionMark) divided by the number of clients. To allow them to go
	 * over that amount, bump this above 100.
	 */
	private final int perClientMaxPercentOfFairBacklogSize;

	/**
	 * per-client maximum backlog size
	 */
	private final int perClientMaxBacklogSize;

	/**
	 * maximum time for a client to wait to add metrics to a backlogged queue
	 * before giving up.
	 */
	private final int maxClientWaitTime;

	/**
	 * Maximum time between retries to accept metrics into a back-logged metric
	 * queue.
	 */
	private final int minTimeBetweenRetries;

	/**
	 * Variable for tracking whether or not the number of collisions is still
	 * going up or if it is going down
	 */
	private final AtomicLong lastCollisionCount;

}
