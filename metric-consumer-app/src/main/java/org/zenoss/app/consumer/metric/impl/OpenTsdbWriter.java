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

import com.google.api.client.util.ExponentialBackOff;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.TsdbMetricsQueue;
import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.TsdbWriterRegistry;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.lib.tsdb.OpenTsdbClient;
import org.zenoss.lib.tsdb.OpenTsdbClientPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

/**
 * @see TsdbWriter
 */
@Component
@Profile("prod")
@Scope("prototype")
class OpenTsdbWriter implements TsdbWriter {

    @Autowired
    OpenTsdbWriter(
        MetricServiceConfiguration config,
        TsdbWriterRegistry registry,
        OpenTsdbClientPool clientPool,
        TsdbMetricsQueue metricsQueue,
        @Qualifier("zapp::event-bus::async") EventBus eventBus
    ) {
        this.clientPool = clientPool;
        this.metricsQueue = metricsQueue;
        this.writerRegistry = registry;
        this.eventBus = eventBus;

        this.batchSize = config.getJobSize();
        this.maxIdleTime = config.getMaxIdleTime();
        this.maxBackOff = config.getMaxConnectionBackOff();
        this.minBackOff = config.getMinConnectionBackOff();

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
        } catch (InterruptedException ie) {
            log.info("Exiting due to thread interrupt");
            Thread.currentThread().interrupt();
        } finally {
            running = false;
            writerRegistry.unregister(this);
        }
    }

    void runUntilCanceled() throws InterruptedException {
        ExponentialBackOff backoffTracker = null;
        while (!isCanceled()) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            Collection<Metric> metrics = metricsQueue.poll(batchSize, maxIdleTime);
            log.debug("Back from polling metricsQueue. metrics.size = {}", null == metrics ? "null" : metrics.size());
            // Check to see if we should down this writer entirely.
            log.debug("Checking for shutdown. lastWorkTime = {}; maxIdleTime = {}; sum = {}; currentTime ={}",
                lastWorkTime, maxIdleTime, lastWorkTime+maxIdleTime, System.currentTimeMillis());
            if (isNullOrEmpty(metrics) &&    // No records could be read from the metrics queue
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
            if (isNullOrEmpty(metrics)) {
                log.debug("No work to do, so checking again.");
                continue;
            }

            // We have some work to do, some process what we got from the metrics queue
            if (backoffTracker == null) {
                backoffTracker = createExponentialBackOff();
            }
            try {
                processBatch(metrics);
                backoffTracker.reset();
            } catch (NoSuchElementException e) {
                long backOff = this.minBackOff;
                try {
                    backOff = backoffTracker.nextBackOffMillis();
                } catch (IOException e1) {
                    // shouldn't happen but if it does we'll use the default backOff
                    log.debug("caught IOException backing off tracker - should go to default backOff.");
                }
                if (ExponentialBackOff.STOP == backOff) {
                    // We've reached the max amount of time to backoff, use max backoff
                    backOff = backoffTracker.getMaxIntervalMillis();
                    log.warn("Error getting OpenTsdbClient after {} ms: {}", backoffTracker.getElapsedTimeMillis(), e.getMessage());
                }
                log.debug("Connection back off, sleeping {} ms", backOff);
                Thread.sleep(backOff);
            }
        }
        log.debug("work canceled.");
    }

    private boolean isNullOrEmpty(Collection<Metric> metrics) {
        return null == metrics || metrics.isEmpty();
    }

    private ExponentialBackOff createExponentialBackOff() {
        return new ExponentialBackOff.Builder().
            setMaxElapsedTimeMillis(this.maxBackOff).
            setMaxIntervalMillis(this.maxBackOff).
            setInitialIntervalMillis(Math.min(this.minBackOff, this.maxBackOff)).
            build();
    }


    void processBatch(Collection<Metric> metrics) throws InterruptedException {
        OpenTsdbClient client = null;
        boolean flushed = false;
        try {
            long processed = 0;
            client = getOpenTsdbClient();
            if (client != null) {
                int errs = clientPool.clearErrorCount();
                if (errs > 0) {
                    metricsQueue.incrementError(errs);
                }

                if ( clientPool.hasCollision()) {
                    eventBus.post(Control.highCollision());
                    throw new NoSuchElementException("Collision detected");
                }

                try {
                    for (Metric m : metrics) {
                        // ZEN-11665 - make copy of metric before messing with it. This prevents side-effect issues when exceptions occur.
                        Metric workingCopy = new Metric(m);
                        workingCopy.removeTag(TsdbMetricsQueue.CLIENT_TAG);
                        try {
                            final String message = convert(workingCopy);
                            client.put(message);
                            processed++;
                        } catch (IOException e) {
                            log.warn("Caught (and rethrowing) IOException while processing metric: {}", e.getMessage());
                            throw e;
                        }
                    }
                    client.flush();
                    flushed = true;
                    metricsQueue.incrementProcessed(processed);
                } catch (IOException e) {
                    log.warn("Caught exception while processing messages: {}", e.getMessage());
                    client.close();
                }
            } else {
                log.warn("Unable to get client to process metrics.");
            }
        } finally {
            if (!flushed) {
                try {
                    metricsQueue.reAddAll(metrics);
                } catch (IllegalStateException e) {
                    log.error("We were unable to add metrics back to the queue. Eating exception to prevent thread death.", e);
                }
            }
            if (client != null) {
                try {
                    clientPool.returnObject(client);
                } catch (Exception returnException) {
                    log.warn("Failed to return TSDB client to connection pool", returnException);
                }
            }
            lastWorkTime = System.currentTimeMillis();
        }
    }

    private OpenTsdbClient getOpenTsdbClient() throws InterruptedException {
        OpenTsdbClient client;
        try {
            client = (OpenTsdbClient) clientPool.borrowObject();
            if ( null == client) {
                log.warn("got null client from pool.");
            } else {
                log.debug(String.format("Got client from pool.  isAlive = %s. isClosed = %s", String.valueOf(client.isAlive()), String.valueOf(client.isClosed())));
            }
        } catch (InterruptedException | NoSuchElementException e) {
            log.warn("Caught exception getting OpenTsdbClient - rethrowing.", e);
            // don't swallow These exceptions
            throw e;
        } catch (Exception e) { // Exception required due to GenericObjectPool.borrowObject
            throw new NoSuchElementException(e.toString());
        }
        return client;
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

    /**
     * where to report in when running
     */
    private final TsdbWriterRegistry writerRegistry;

    /**
     * where the clients come from
     */
    private final OpenTsdbClientPool clientPool;

    /**
     * unprocessed data to write into TSDB
     */
    protected final TsdbMetricsQueue metricsQueue;

    /**
     * EventBus for broadcasting tsdb collision detection
     */
    protected final EventBus eventBus;

    /**
     * Size of batches to send to TSDB socket
     */
    private final int batchSize;

    /**
     * Max idle time before suicide
     */
    private final int maxIdleTime;

    /**
     * Max time for connection backoff
     */
    private final int maxBackOff;

    /**
     * Min time for connection backoff
     */
    private final int minBackOff;

    /**
     * Is this instance currently running?
     */
    private transient boolean running;

    /**
     * Has this instance been canceled?
     */
    private transient boolean canceled;

    /**
     * Last time this instance did work
     */
    protected transient long lastWorkTime;


    private static final Pattern INVALID_CHARS = Pattern.compile("[^\\w\\./_-]");

    static final String sanitize(String input) {
        return INVALID_CHARS.matcher(input).replaceAll("-");
    }

    static final String convert(Metric metric) {
        String name = metric.getMetric();
        long timestamp = metric.getTimestamp();
        double value = metric.getValue();
        Map<String, String> tags = Maps.newHashMap();

        for (Entry<String, String> entry : metric.getTags().entrySet()) {
            tags.put(sanitize(entry.getKey()), sanitize(entry.getValue()));
        }

        return OpenTsdbClient.toPutMessage(name, timestamp, value, tags);
    }

}
