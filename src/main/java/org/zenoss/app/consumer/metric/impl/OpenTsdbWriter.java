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

import org.zenoss.app.consumer.metric.TsdbWriter;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
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
            MetricsQueue metricsQueue)
    {
        this.clientPool = clientPool;
        this.metricsQueue = metricsQueue;
        this.writerRegistry = registry;

        this.batchSize = config.getJobSize();
        this.sleepWhenEmpty = config.getSleepWhenEmpty();
        this.maxIdleTime = config.getMaxIdleTime();

        this.timePerBatch = Metrics.newTimer(new MetricName(OpenTsdbWriter.class, "timePerBatch"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        this.running = false;
        this.canceled = false;
        this.lastWorkTime = System.currentTimeMillis();
    }
    
    @Override
    public void run() {
        log.info("Starting writer");
        try {
            writerRegistry.register(this);
            running = true;
            while (!isCanceled()) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                
                OpenTsdbClient client = null;
                try {
                    long processed = 0;
                    client = clientPool.borrowObject();
                    
                    int errs = clientPool.clearErrorCount();
                    if (errs > 0) {
                        metricsQueue.incrementError(errs);
                    }
                    
                    while (processed < 1_000_000) {
                        Collection<Metric> metrics = metricsQueue.poll(batchSize);
                        for (Metric m : metrics) {
                            String message = convert(m);
                            client.put(message);
                            processed++;
                            metricsQueue.incrementProcessed(1);
                        }
                    }
                    client.flush();
                } 
                catch (Exception e) {
                    log.warn("Caught unexpected exception processing messages", e);
                    //metricsQueue.addAll(metrics, true);
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
                    client = null;
//                    batchTimeContext.stop();
                    lastWorkTime = System.currentTimeMillis();
                }
            }
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
    private final MetricsQueue metricsQueue;
    
    /** Size of batches to send to TSDB socket */
    private final int batchSize;

    /** Max idle time before suicide */
    private final int maxIdleTime;
    
    /** Time to rest when there is no work */
    private final int sleepWhenEmpty;

    /** The time it takes to process a batch from the publisher */
    private final Timer timePerBatch;
    
    /** Is this instance currently running? */
    private transient boolean running;
    
    /** Has this instance been canceled? */
    private transient boolean canceled;
    
    /** Last time this instance did work */
    private transient long lastWorkTime;
}
