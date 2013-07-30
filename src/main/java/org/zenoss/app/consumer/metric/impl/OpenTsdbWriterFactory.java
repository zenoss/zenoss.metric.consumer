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
class OpenTsdbWriterFactory implements TsdbWriterFactory {
    
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
}
