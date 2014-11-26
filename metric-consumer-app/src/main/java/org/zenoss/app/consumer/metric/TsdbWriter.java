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
package org.zenoss.app.consumer.metric;

/**
 * Responsible for writing data to TSDB in the background.
 */
public interface TsdbWriter extends Runnable {

    /**
     * Inform the writer that it should immediately stop work and shut down.
     */
    void cancel();
    
    /**
     * Check whether the writer is currently running
     * @return true if the writer is running, false otherwise
     */
    boolean isRunning();
    
}
