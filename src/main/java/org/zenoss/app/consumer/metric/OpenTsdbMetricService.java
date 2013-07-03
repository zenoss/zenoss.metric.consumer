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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.dropwizardspring.annotations.Managed;

@Managed
public class OpenTsdbMetricService implements MetricService, com.yammer.dropwizard.lifecycle.Managed {

    static final Logger log = LoggerFactory.getLogger(OpenTsdbMetricService.class);

    @Autowired
    public OpenTsdbMetricService(ConsumerAppConfiguration config) {
        this( config, new OpenTsdbExecutorService( config.getMetricServiceConfiguration()));
    }

    public OpenTsdbMetricService(ConsumerAppConfiguration config, OpenTsdbExecutorService executorService) {
        this.config = config;
        this.executorService = executorService;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() throws InterruptedException {
        executorService.stop();
    }

    @Override
    public Control push(Metric[] metrics) {
        if ( metrics == null) {
            //yuck
            return new Control( );
        }

        if ( backoff( metrics.length)) {
            //hold your horses bud!
            return new Control();
        }

        //yee haw - all your metrics are belong to us
        int i = 0;
        int jobSize = config.getMetricServiceConfiguration().getJobSize();
        while ( i < metrics.length && i + jobSize > 0) {
            int end = i + jobSize;
            if ( end > metrics.length) {
                end = metrics.length;
            }
            executorService.submit( this, metrics, i, end);
            i += jobSize;
        }

        if ( i < metrics.length) {
            //this may happen on overflow
            executorService.submit( this, metrics, i, metrics.length);
        }

        return new Control();
    }

    /**
     * @ return how many messages were queued
     */
    public synchronized long getTotalIncoming() {
        return totalIncoming;
    }

    /**
     * @ return how many messages were written
     */
    public synchronized long getTotalOutgoing() {
        return totalOutgoing;
    }

    /**
     * @ return how many errors did OpenTsdb return
     */
    public synchronized long getTotalErrors() {
        return totalErrors;
    }

    /** backoff test and increment */
    synchronized boolean backoff( int v) {
        if ( v >= 0) {
            if ( totalIncoming + v < 0) {
                //reset the counter
                totalIncoming = totalIncoming - totalOutgoing;
                totalOutgoing = 0;
            }

            //edge case
            if ( totalIncoming + v < 0) {
                return true;
            }

            totalIncoming += v;

            return false;
        }
        return true;
    }

    //exposed for testing
    synchronized void incrementTotalIncoming(int v) {
        if ( v >= 0) {
            totalIncoming += v;
        }
    }

    synchronized void incrementTotalOutgoing(int v) {
        if ( v >= 0) {
            totalOutgoing += v;
            //backoff handles overflow...
        }
    }

    synchronized void incrementTotalError(int v) {
        if ( v >= 0) {
            totalErrors += v;
            //wrap around back to zero, because, opentsdb may not like our metrics :-(
            if (totalErrors < 0) {
                totalErrors = v;
            }
        }
    }

    /**
     * application configuration
     */
    private ConsumerAppConfiguration config;

    /**
     * manage opentsd read/write jobs
     */
    private OpenTsdbExecutorService executorService;

    /**
     * counter for total messages pushed
     */
    private long totalIncoming = 0;

    /**
     * counter for total messages send to OpenTsdb
     */
    private long totalOutgoing = 0;

    /**
     * counter for total errors from OpenTsdb
     */
    private long totalErrors = 0;
}
