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

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.TsdbWriterRegistry;
import org.zenoss.dropwizardspring.annotations.Managed;

import java.util.Collection;


/**
 * This class tracks currently running TSDB writer threads and informs them 
 * when the dropwizard container is shutting down.
 */
@Managed
class OpenTsdbWriterRegistry implements TsdbWriterRegistry, io.dropwizard.lifecycle.Managed {
    
    private static final Logger log = LoggerFactory.getLogger(OpenTsdbWriterRegistry.class);

    OpenTsdbWriterRegistry() {
        this.createdWriters = Lists.newCopyOnWriteArrayList();
    }

    @Override
    public void register(TsdbWriter writer) {
        createdWriters.add(writer);
    }

    @Override
    public void unregister(TsdbWriter writer) {
        createdWriters.remove(writer);
    }

    @Override
    public int size() {
        return createdWriters.size();
    }
    
    @Override
    public void start() throws Exception {
        log.debug("Starting");
    }

    @Override
    public synchronized void stop() throws Exception {
        int count=0;
        for (TsdbWriter writer : createdWriters) {
            writer.cancel();
            count++;
        }
        log.info("Shutdown {} writer(s)", count);
    }
    
    // State
    private final Collection<TsdbWriter> createdWriters;
}
