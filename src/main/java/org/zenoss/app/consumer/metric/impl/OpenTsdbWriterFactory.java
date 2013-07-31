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

import com.yammer.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.app.consumer.metric.TsdbWriterFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedList;
import org.zenoss.app.consumer.metric.TsdbWriter;

/**
 *
 * @author cschellenger
 */
@Component
class OpenTsdbWriterFactory implements TsdbWriterFactory, Managed {
    
    static final Logger log = LoggerFactory.getLogger(OpenTsdbWriterFactory.class);
    
    @Autowired
    OpenTsdbWriterFactory(ApplicationContext appContext)
    {
        this.appContext = appContext;
        this.createdWriters = new LinkedList<>();
    }
    
    @Override
    public TsdbWriter createWriter() {
        TsdbWriter writer = appContext.getBean(TsdbWriter.class);
        createdWriters.add(writer);
        return writer;
    }
    
    @Override
    public Collection<TsdbWriter> getCreatedWriters() {
        return createdWriters;
    }
    
    private final ApplicationContext appContext;
    private final Collection<TsdbWriter> createdWriters;

    @Override
    public void start() throws Exception {
        log.debug("Starting");
    }

    @Override
    public void stop() throws Exception {
        int count=0;
        for (TsdbWriter writer : getCreatedWriters()) {
            writer.cancel();
            count++;
        }
        log.info("Shutdown {} writer(s)", count);
    }
}
