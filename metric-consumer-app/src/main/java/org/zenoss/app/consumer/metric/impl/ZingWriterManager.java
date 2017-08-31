/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2017, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */
package org.zenoss.app.consumer.metric.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.zenoss.app.consumer.metric.ZingConfiguration;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Manages the number of threads writing to Zing.
 */
@Component
public class ZingWriterManager implements Runnable  {
    private static final Logger logger = LoggerFactory.getLogger(ZingWriterManager.class);
    private static int ALLOWED_LAG_CYCLES = 3;
    private ApplicationContext appContext = null;
    private ExecutorService executorService = null;
    private ScheduledExecutorService scheduledExecutorService = null;
    private ZingQueue zingQueue = null;
    private ZingConfiguration zingConfiguration = null;
    private int maxWriterThreads = 1;
    private ScheduledFuture<?> scheduledTask = null;
    private ZingWriterRegistry writers = null;

    @Autowired
    ZingWriterManager(ApplicationContext appContext,
                      MetricServiceConfiguration config,
                      ZingQueue zingQueue,
                      ZingWriterRegistry writerRegistry,
                      @Qualifier("zapp::executor::zing") ExecutorService executorService,
                      @Qualifier("zapp::executor::scheduled") ScheduledExecutorService scheduledExecutorService) {
        this.appContext = appContext;
        this.executorService = executorService;
        this.scheduledExecutorService = scheduledExecutorService;
        this.zingQueue = zingQueue;
        this.writers = writerRegistry;
        this.zingConfiguration = config.getZingConfiguration();
        this.maxWriterThreads = zingConfiguration.getWriterThreads();
    }

    @PostConstruct
    public void schedule() {
        // There's no need to spin up threads that will never be called
        if (!this.zingConfiguration.isEnabled()) {
            logger.debug("Zing integration disabled - not scheduling anything");
            return;
        }

        if (this.scheduledTask == null) {
            logger.debug("Scheduling ZingWriterManager");

            this.scheduledTask = scheduledExecutorService.scheduleWithFixedDelay(this, 0L, 30,
                    TimeUnit.SECONDS);
        } else {
            logger.warn("Attempt to re-schedule ZingWriterManager!");
        }
    }

    public void run() {
        int needed = needMoreWriters();
        int created = 0;

        while (needed-- > 0) {
            ZingWriter writer = appContext.getBean(ZingWriter.class);
            this.executorService.submit(writer);
            ++created;
        }
        logger.debug("Created {} writers", created);
    }

    private int needMoreWriters() {
        //
        int current = writers.size();
        int needed = 0;

        if (current == 0) {
            // first time around
            needed = 1;
        } else {
            int lagCycles = Math.floorDiv(this.zingQueue.size(), current * this.zingConfiguration.getBatchSize());
            if (lagCycles > ALLOWED_LAG_CYCLES) {
                needed = 2 * current;
            }
        }
        logger.debug("Writers needed: {}", needed);

        if (needed > 0) {
            if (current >= this.maxWriterThreads) {
                logger.debug("Current writers count exceeds allowed: {}", this.maxWriterThreads);
                needed = 0;
            } else if ((current + needed) > maxWriterThreads) {
                needed = maxWriterThreads - current;
                logger.debug("Current + needed writers count exceeds allowed: {}. Adjusting needed to {}",
                        this.maxWriterThreads, needed);
            }
        }

        return needed;
    }
}
