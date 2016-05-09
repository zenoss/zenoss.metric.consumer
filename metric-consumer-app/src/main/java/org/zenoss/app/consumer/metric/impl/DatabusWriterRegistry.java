package org.zenoss.app.consumer.metric.impl;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.dropwizardspring.annotations.Managed;

import java.util.Collection;



@Managed
class DatabusWriterRegistry implements com.yammer.dropwizard.lifecycle.Managed {
    
    private static final Logger log = LoggerFactory.getLogger(OpenTsdbWriterRegistry.class);

    DatabusWriterRegistry() {
        this.createdWriters = Lists.newCopyOnWriteArrayList();
    }

    
    public void register(DatabusWriter writer) {
        createdWriters.add(writer);
    }

   
    public void unregister(DatabusWriter writer) {
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
        int count=0;
        for (DatabusWriter writer : createdWriters) {
            writer.cancel();
            count++;
        }
        log.info("Shutdown {} writer(s)", count);
    }
    
    // State
    private final Collection<DatabusWriter> createdWriters;
}



