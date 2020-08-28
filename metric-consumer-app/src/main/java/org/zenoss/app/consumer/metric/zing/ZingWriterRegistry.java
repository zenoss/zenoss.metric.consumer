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
package org.zenoss.app.consumer.metric.zing;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.dropwizardspring.annotations.Managed;

import java.util.Collection;


/**
 * This class tracks currently running Zing writer threads and informs them
 * when the dropwizard container is shutting down.
 */
@Managed
class ZingWriterRegistry implements io.dropwizard.lifecycle.Managed {

    private static final Logger log = LoggerFactory.getLogger(ZingWriterRegistry.class);

    ZingWriterRegistry() {
        this.createdWriters = Lists.newCopyOnWriteArrayList();
    }


    public void register(ZingWriter writer) {
        log.debug("Adding writer");
        createdWriters.add(writer);
    }


    public void unregister(ZingWriter writer) {
        log.debug("Removing writer");
        createdWriters.remove(writer);
    }

    public int size() {
        return createdWriters.size();
    }

    @Override
    public void start() throws Exception {
        log.debug("Starting");
    }

    @Override
    public synchronized void stop() throws Exception {
        int count = 0;
        for (ZingWriter writer : createdWriters) {
            writer.cancel();
            count++;
        }
        log.info("Shutdown {} writer(s)", count);
    }

    // State
    private final Collection<ZingWriter> createdWriters;
}



