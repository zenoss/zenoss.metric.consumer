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
package org.zenoss.app.consumer.metric;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

/**
 * Factory used to create and manage TSDB writer instances.
 */
public interface TsdbWriterFactory {

    /**
     * Create a new instance of a TSDB writer. The writer must be submitted to
     * an {@link ExecutorService} to operate correctly.
     * 
     * @return instance
     */
    TsdbWriter createWriter();
    
    /**
     * Returns a collection of all TSDB writers created by this factory. This 
     * collection is mutable and not threadsafe.
     * 
     * @return created writer instances
     */
    Collection<TsdbWriter> getCreatedWriters();
    
}
