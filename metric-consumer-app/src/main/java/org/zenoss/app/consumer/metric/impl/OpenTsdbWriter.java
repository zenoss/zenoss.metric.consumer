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
package org.zenoss.app.consumer.metric.impl;

import com.google.api.client.util.ExponentialBackOff;
import com.google.common.base.Strings;
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
        } catch (RuntimeException e) {
            log.error("Thread exiting due to unexpected exception", e);
            throw e;
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
        boolean invalidateClient = false;
        long processed = 0;
        try {
            client = getOpenTsdbClient();
            if (client != null) {
                int errs = clientPool.clearErrorCount();
                if (errs > 0) {
                    metricsQueue.incrementError(errs);
                }

                if ( clientPool.hasCollision()) {
                    // Note: calling .hasCollision() also had the side-effect of clearing it.
                    invalidateClient = true;
                    eventBus.post(Control.highCollision());
                    throw new NoSuchElementException("Collision detected");
                }

                try {
                    for (Metric m : metrics) {
                        // ZEN-11665 - make copy of metric before messing with it. This prevents side-effect issues when exceptions occur.
                        Metric workingCopy = new Metric(m);
                        workingCopy.removeTag(TsdbMetricsQueue.CLIENT_TAG);
                        String message = null;
                        try {
                            message = convert(workingCopy);
                        } catch (RuntimeException e) {
                            if (log.isDebugEnabled()) {
                                log.warn(String.format("Dropping bad metric : %s : %s", e.getMessage(), workingCopy.toString()), e);
                            } else {
                                log.warn("Dropping bad metric : {} : {}", e.getMessage(), workingCopy);
                            }
                            processed++;
                        }
                        if (message != null) {
                            log.trace("Publishing metric: {}", m.toString());
                            try {
                                client.put(message);
                                processed++;
                            } catch (IOException e) {
                                log.warn("Caught (and rethrowing) IOException while processing metric: {}", e.getMessage());
                                throw e;
                            }
                        }
                    }
                    boolean anyErrors = false;
                    for (String error : client.checkForErrors()) {
                        log.warn("OpenTSDB returned an error: {}", error);
                        anyErrors = true;
                    }
                    if (anyErrors) {
                        invalidateClient = true;
                    } else {
                        flushed = true;
                    }
                } catch (IOException e) {
                    log.warn("Caught exception while processing messages: {}", e.getMessage());
                    invalidateClient = true;
                }
            } else {
                log.warn("Unable to get client to process metrics.");
            }
        } finally {
            if (flushed) {
                metricsQueue.incrementProcessed(processed);
            } else {
                try {
                    metricsQueue.reAddAll(metrics);
                } catch (Exception e) {
                    log.error("We were unable to add metrics back to the queue. Eating exception to prevent thread death.", e);
                    metricsQueue.incrementLostMetrics(metrics.size());
                }
            }
            if (client != null) {
                try {
                    if (invalidateClient)
                        clientPool.invalidateObject(client);
                    else
                        clientPool.returnObject(client);
                } catch (Exception releaseException) {
                    log.warn("Error while releasing TSDB client", releaseException);
                }
            }
            lastWorkTime = System.currentTimeMillis();
        }
    }

    private OpenTsdbClient getOpenTsdbClient() throws InterruptedException {
        OpenTsdbClient client = null;
        try {
            client = (OpenTsdbClient) clientPool.borrowObject();
            if ( null == client) {
                log.warn("got null client from pool.");
            } else {
                log.debug(String.format("Got client from pool.  isAlive = %s. isClosed = %s", String.valueOf(client.isAlive()), String.valueOf(client.isClosed())));
            }
            return client;
        } catch (InterruptedException | NoSuchElementException e) {
            if (client != null) {
                try {
                    clientPool.invalidateObject(client);
                } catch (Exception releaseException) {
                    log.warn("Error while releasing TSDB client", releaseException);
                }
            }
            log.warn("Caught exception getting OpenTsdbClient - rethrowing.", e);
            // don't swallow These exceptions
            throw e;
        } catch (Exception e) { // Exception required due to GenericObjectPool.borrowObject
            log.warn("Caught exception getting OpenTsdbClient - wrapping and rethrowing.", e);
            if (client != null) {
                try {
                    clientPool.invalidateObject(client);
                } catch (Exception releaseException) {
                    log.warn("Error while releasing TSDB client", releaseException);
                }
            }
            throw new NoSuchElementException(e.toString());
        }
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
        if (input == null) return null;
        return INVALID_CHARS.matcher(input).replaceAll("-");
    }

    protected static final String SPACE_REPLACEMENT = "//-";

    static final String convert(Metric metric) {
        String name = metric.getMetric();
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("missing name");
        }
        long timestamp = metric.getTimestamp();
        double value = metric.getValue();
        if (Double.isNaN(value)) {
            throw new IllegalArgumentException("Value is NaN: %s" + metric.toString());
        }

        Map<String, String> tags = Maps.newHashMap();
        for (Entry<String, String> entry : metric.getTags().entrySet()) {
            String tagKey = sanitize(entry.getKey());
            String tagValue = sanitize(entry.getValue());
            if (tagKey == null) {
                if (tagValue != null)
                    log.warn("empty tag key, dropping tag null:{} for metric {}", tagValue, metric);
                continue;
            } else if (tagValue == null) {
                log.warn("empty tag value, dropping tag: {}:null for metric {}", tagKey, metric);
                continue;
            }
            tags.put(tagKey, tagValue);
        }

        // escape spaces from the metric name since space is an invalid OpenTSDB metric name
        name = name.replace(" ", SPACE_REPLACEMENT);
        name = sanitize(name);

        return OpenTsdbClient.toPutMessage(name, timestamp, value, tags);
    }

}
