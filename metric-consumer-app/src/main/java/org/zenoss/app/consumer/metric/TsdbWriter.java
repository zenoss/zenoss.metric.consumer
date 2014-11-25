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
