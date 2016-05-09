/**
 * 
 */
package org.zenoss.app.consumer.metric.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.eclipse.jetty.util.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.zenoss.app.consumer.metric.DatabusPublishConfig;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;

/**
 *
 *
 */
@Component
public class DatabusWriterManager {
	private static final Logger logger = LoggerFactory.getLogger(DatabusWriterManager.class);
	private static int ALLOWED_LAG_CYCLES = 3;
	private ApplicationContext appContext = null;
	private ExecutorService executorService = null;
	private ScheduledExecutorService scheduledExecutorService = null;
	private DatabusMetricsQueue databusMetricsQueue = null;
	private DatabusPublishConfig databusConfig = null;
	private int minTimeBetweenChecks = 30000; // milliseconds
	private int databusWriterThreads = 1;
	private ScheduledFuture<?> scheduledTask = null;
	private DatabusWriterRegistry writers = null;

	@Autowired
	DatabusWriterManager(ApplicationContext appContext, MetricServiceConfiguration config,
			DatabusMetricsQueue databusMetricsQueue, DatabusWriterRegistry dbwriterRegistry,
			@Qualifier("zapp::executor::databus") ExecutorService executorService,
			@Qualifier("zapp::executor::scheduled") ScheduledExecutorService scheduledExecutorService) {
		this.appContext = appContext;
		this.executorService = executorService;
		this.scheduledExecutorService = scheduledExecutorService;
		this.databusMetricsQueue = databusMetricsQueue;
		this.writers = dbwriterRegistry;
		this.databusConfig = config.getDatabusPublishConfig();
		this.minTimeBetweenChecks = databusConfig.getMaxIdleTime();
		this.databusWriterThreads = databusConfig.getDatabusWriterThreads();

		// this.lastCheckTime = new AtomicLong();
	}

	@PostConstruct
	public void schedule() {
		if (this.scheduledTask == null) {
			logger.debug("Scheduling DatabusWriterManager");

			this.scheduledTask = scheduledExecutorService.scheduleWithFixedDelay(this::createWriters, 0L, 30,
					TimeUnit.SECONDS);
		} else {
			logger.warn("Attempt to re-schedule DatabusWriterManager!");
		}
	}

	public void createWriters() {
		int needed = needMoreWriters();
		int created = 0;

		while (needed-- > 0) {
			DatabusWriter writer = appContext.getBean(DatabusWriter.class);
			this.executorService.submit(writer);
			++created;
		}
		logger.debug("Created {} databus writers", created);
	}

	private int needMoreWriters() {
		//
		int current = writers.size();
		int needed = 0;

		if (current == 0) {
			// first time around
			needed = 1;
		} else {
			int lagCycles = Math.floorDiv(this.databusMetricsQueue.size(), current * this.databusConfig.getBatchSize());
			logger.debug("lagCycles: {}", lagCycles);
			if (lagCycles > ALLOWED_LAG_CYCLES) {
				needed = 2 * current;
			}
		}
		logger.debug("Writers needed: {}", needed);

		if (needed > 0) {
			if (current >= this.databusWriterThreads) {
				logger.debug("Current + needed writers count exceeds allowed: {}", this.databusWriterThreads);
				needed = 0;
			} else if ((current + needed) > databusWriterThreads) {
				needed = databusWriterThreads - current;
				logger.debug("Current + needed writers count exceeds allowed: {}. Adjusting needed to {}",
						this.databusWriterThreads, needed);

			}
		}

		return needed;
	}
}
