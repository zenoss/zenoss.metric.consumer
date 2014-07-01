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
import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.TsdbWriterRegistry;
import org.zenoss.app.consumer.metric.data.Control;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Subscribes to EventBus messages to ensure an appropriate number of TSDB 
 * writer threads are running.
 */
@Component
class OpenTsdbWriterManager {
    
    private static final Logger log = LoggerFactory.getLogger(OpenTsdbWriterManager.class);
    
    @Autowired
    OpenTsdbWriterManager(
            ApplicationContext appContext, 
            MetricServiceConfiguration config, 
            @Qualifier("zapp::event-bus::async") EventBus eventBus, 
            @Qualifier("zapp::executor::metrics") ExecutorService executorService,
            TsdbWriterRegistry writerRegistry)
    {
        this.appContext = appContext;
        this.executorService = executorService;
        this.eventBus = eventBus;
        this.writerRegistry = writerRegistry;
        
        this.minTimeBetweenChecks = config.getMaxIdleTime();
        this.tsdbWriterThreads = config.getTsdbWriterThreads();
        
        this.lastCheckTime = new AtomicLong();
    }
    
    @PostConstruct
    public void listenToEvents() {
        eventBus.register(this);
    }
    
    @Subscribe
    public void processControl(Control event) {
        log.debug("Received event {}", event.getType());
        switch (event.getType()) {
            case LOW_COLLISION:
            case HIGH_COLLISION:
            case DATA_RECEIVED:
                long now = System.currentTimeMillis();
                long lastCheckTimeExpected = lastCheckTime.get();
                
                if (now > lastCheckTimeExpected + minTimeBetweenChecks &&
                        lastCheckTime.compareAndSet(lastCheckTimeExpected, now)) {
                    createWriters();
                }
                break;
                
            default:
        }
    }
    
    void createWriters() {
        log.debug("createWriters(): tsdbWriterThreads = {}, writerRegistry.size() = {}. WritersToCreate = {}",
            tsdbWriterThreads, writerRegistry.size(), tsdbWriterThreads - writerRegistry.size());
        final int writersToCreate = tsdbWriterThreads - writerRegistry.size();
        int created = 0;

        while (created < writersToCreate) {
            TsdbWriter writer = appContext.getBean(TsdbWriter.class);
            log.debug("createWriters(): new writer: {}", writer.toString());
            executorService.submit(writer);
            created++;
        }
        
        if (created > 0) {
            log.info("Created {} new writers", created);
        }
    }
    
    // Dependencies
    private final ApplicationContext appContext;
    private final ExecutorService executorService;
    private final EventBus eventBus;
    private final TsdbWriterRegistry writerRegistry;
    
    // Configuration
    private final int tsdbWriterThreads;
    private final long minTimeBetweenChecks;
    
    // State
    private final AtomicLong lastCheckTime;
}
