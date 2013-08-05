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

/**
 * Shared data structure containing references to all currently running TSDB 
 * writer threads.
 */
public interface TsdbWriterRegistry {
    
    void register(TsdbWriter writer);
    void unregister(TsdbWriter writer);
    int size();
}
