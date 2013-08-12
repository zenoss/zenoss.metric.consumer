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

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.TsdbMetricsQueue;
import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.TsdbWriterRegistry;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.lib.tsdb.OpenTsdbClient;
import org.zenoss.lib.tsdb.OpenTsdbClientPool;

/**
 * @see TsdbWriter
 */
@Component
@Scope("prototype")
class OpenTsdbWriter implements TsdbWriter {

    @Autowired
    OpenTsdbWriter(
            MetricServiceConfiguration config, 
            TsdbWriterRegistry registry,
            OpenTsdbClientPool clientPool, 
            TsdbMetricsQueue metricsQueue)
    {
        this.clientPool = clientPool;
        this.metricsQueue = metricsQueue;
        this.writerRegistry = registry;

        this.batchSize = config.getJobSize();
        this.maxIdleTime = config.getMaxIdleTime();
        
        this.running = false;
        this.canceled = false;
        this.lastWorkTime = 0;
    }
    
    @Override
    public void run() {
        log.info("Starting writer");
        try {
            writerRegistry.register(this);
            running = true;
            runUntilCanceled();
        }
        catch(InterruptedException ie) {
            log.info("Exiting due to thread interrupt");
            Thread.currentThread().interrupt();
        }
        finally {
            running = false;
            writerRegistry.unregister(this);
        }
    }

    void runUntilCanceled() throws InterruptedException{
        while (!isCanceled()) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            Collection<Metric> metrics = metricsQueue.poll(batchSize, maxIdleTime);

            // Check to see if we should down this writer entirely.
            if (metrics.isEmpty() &&    // No records could be read from the metrics queue
                    lastWorkTime > 0 && // This thread has done work at least once
                    maxIdleTime > 0 &&  // The max idle time is set to something meaningful
                    System.currentTimeMillis() > lastWorkTime + maxIdleTime) // The max idle time has expired
            {
                log.info("Shutting down writer due to dearth of work");
                break;
            }

            /*
             * If all the conditions were not met for shutting this writer down,
             * we still might want to just abort this run if we didn't get any
             * data from the metrics queue
             */
            if (metrics.isEmpty()) {
                log.debug("No work to do, so checking again.");
                continue;
            }

            // We have some work to do, some process what we got from the metrics queue
            processBatch(metrics);
        }
    }

    void processBatch(Collection<Metric> metrics) {
        OpenTsdbClient client = null;

        try {
            long processed = 0;
            client = clientPool.borrowObject();

            int errs = clientPool.clearErrorCount();
            if (errs > 0) {
                metricsQueue.incrementError(errs);
            }

            for (Metric m : metrics) {
                String message = convert(m);
                client.put(message);
                processed++;
            }

            metricsQueue.incrementProcessed(processed);
            client.flush();
        }
        catch (Exception e) { // Exception required due to GenericObjectPool.borrowObject
            log.warn("Caught unexpected exception processing messages", e);
            metricsQueue.addAll(metrics, true);

            if (client != null) {
                client.close();
            }
        }
        finally {
            if (client != null) {
                try {
                    clientPool.returnObject(client);
                }
                catch(Exception returnException) {
                    log.warn("Failed to return TSDB client to connection pool", returnException);
                }
            }
            lastWorkTime = System.currentTimeMillis();
        }
    }
    
    // TODO: Test directly
    String convert(Metric metric) {
        String name = metric.getMetric();
        long timestamp = metric.getTimestamp();
        double value = metric.getValue();
        Map<String, String> tags = metric.getTags();
        return OpenTsdbClient.toPutMessage(name, timestamp, value, tags);
    }
    
    @Override
    public boolean isRunning() {
        return running;
    }
    
    private synchronized boolean isCanceled() {
        return canceled;
    }
    
    @Override
    public synchronized void cancel() {
        log.info("Writer shutdown requested");
        this.canceled = true;
    }
    
    private static final Logger log = LoggerFactory.getLogger(OpenTsdbWriter.class);

    /** where to report in when running */
    private final TsdbWriterRegistry writerRegistry;
    
    /** where the clients come from */
    private final OpenTsdbClientPool clientPool;
    
    /** unprocessed data to write into TSDB */
    private final TsdbMetricsQueue metricsQueue;
    
    /** Size of batches to send to TSDB socket */
    private final int batchSize;

    /** Max idle time before suicide */
    private final int maxIdleTime;
    
    /** Is this instance currently running? */
    private transient boolean running;
    
    /** Has this instance been canceled? */
    private transient boolean canceled;
    
    /** Last time this instance did work */
    private transient long lastWorkTime;
}
