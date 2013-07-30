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
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.TsdbWriterFactory;
import org.zenoss.app.consumer.metric.data.Control;

/**
 * This class is responsible for ensuring an appropriate number of TsdbWriter
 * threads are running.
 * 
 * @author cschellenger
 */
@Component
class OpenTsdbWriterThreadDoctor {
    
    @Autowired
    OpenTsdbWriterThreadDoctor(
            MetricServiceConfiguration config,
            MetricService metricService, 
            TsdbWriterFactory writerFactory,
            @Qualifier("zapp::event-bus::async") EventBus eventBus) 
    {
        this.metricService = metricService;
        this.writerFactory = writerFactory;
        this.eventBus = eventBus;
        this.tsdbWriterThreads = config.getTsdbWriterThreads();
        this.minTimeBetweenChecks = config.getMinTimeBetweenDoctorChecks();
    }
    
    @PostConstruct
    public void registerSelf() {
        eventBus.register(this);
    }
    
    @Subscribe
    public void handle(Control event) {
        switch(event.getType()) {
            case LOW_COLLISION:
            case HIGH_COLLISION:
                
                if (System.currentTimeMillis() > lastCheckTime + minTimeBetweenChecks) {
                    lastCheckTime = System.currentTimeMillis();
                    int running = 0;
                    for (TsdbWriter writer : writerFactory.getCreatedWriters()) {
                        if (writer.isRunning()) {
                            running++;
                        }
                    }
                    
                    while (running < tsdbWriterThreads) {
                        log.info("Starting new writer thread");
                        metricService.startWriter();
                        running++;
                    }
                }
                break;
                
            default:
        }
    }
    
    private static final Logger log = LoggerFactory.getLogger(OpenTsdbWriterThreadDoctor.class);
    
    private final TsdbWriterFactory writerFactory;
    private final MetricService metricService;
    private final EventBus eventBus;
    
    private final int tsdbWriterThreads;
    private final long minTimeBetweenChecks;
    private long lastCheckTime;
}
