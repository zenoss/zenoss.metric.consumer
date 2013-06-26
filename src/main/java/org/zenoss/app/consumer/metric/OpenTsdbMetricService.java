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
import org.zenoss.dropwizardspring.annotations.HealthCheck;
import org.zenoss.dropwizardspring.annotations.Managed;
import org.zenoss.lib.tsdb.OpenTsdbSocketClient;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Managed
public class OpenTsdbMetricService implements MetricService, com.yammer.dropwizard.lifecycle.Managed {

    static final Logger log = LoggerFactory.getLogger(OpenTsdbMetricService.class);

    @Autowired
    ConsumerAppConfiguration config;

    /**
     * incoming message buffer
     */
    BlockingQueue<String> inputBuffer;

    /**
     * OpenTsdb client
     */
    OpenTsdbSocketClient client;

    /**
     * consumer thread for output messages
     */
    MetricWriterThread writer = new MetricWriterThread();

    /**
     * consumer for opentsdb server responses
     */
    MetricReaderThread reader = new MetricReaderThread();

    /**
     * counter for total messages pushed
     */
    AtomicLong totalIncoming = new AtomicLong(0);

    /**
     * counter for total messages send to OpenTsdb
     */
    AtomicLong totalOutgoing = new AtomicLong(0);

    /**
     * counter for total errors from OpenTsdb
     */
    AtomicLong totalErrors = new AtomicLong(0);

    /**
     * condition variable for client-server liveliness
     */
    AtomicBoolean alive = new AtomicBoolean(false);

    @Override
    public void start() throws IOException {
        MetricServiceConfiguration metricConfig = config.getMetricServiceConfiguration();
        inputBuffer = new ArrayBlockingQueue<>(metricConfig.getInputBufferSize());
        client = metricConfig.newClient();

        client.open();

        writer.start();
        reader.start();
    }

    @Override
    public void stop() {
        reader.interrupt();
        writer.interrupt();

        try {
            writer.join();
        } catch (InterruptedException ex) {
        }

        //because opentsdb uses a buffered reader, the socket has to close to interrupt
        client.close();
        try {
            reader.join();
        } catch (InterruptedException ex) {
        }
        alive.set(false);
    }

    @Override
    public Control push(Metric metric) {
        //test for connectivity
        if (!isAlive()) {
            //connection's down, try another client
            return new Control();
        }

        //create put message
        String name = metric.getName();
        long timestamp = metric.getTimestamp();
        double value = metric.getValue();
        Map<String, String> tags = metric.getTags();
        String message = OpenTsdbSocketClient.toPutMessage(name, timestamp, value, tags);

        //enqueue message
        if (inputBuffer.offer(message)) {
            //success
            totalIncoming.incrementAndGet();
            return new Control();
        } else {
            //backoff -- buffer's full
            return new Control();
        }
    }

    /**
     * @return true/false depending on the connection state of tsdb client
     */
    @HealthCheck
    public boolean isAlive() {
        return alive.get();
    }

    /**
     * @ return how many messages are were queued
     */
    public long getTotalIncoming() {
        return totalIncoming.get();
    }

    /**
     * @ return how many messages are were written
     */
    public long getTotalOutgoing() {
        return totalOutgoing.get();
    }

    /**
     * @ return how many errors OpenTsdb returned
     */
    public long getTotalErrors() {
        return totalErrors.get();
    }

    /**
     * @ return how many messages are waiting to be written
     */
    public long getTotalPending() {
        return inputBuffer.size();
    }

    /**
     * A thread to asynchronously write OpenTsdb metrics
     */
    class MetricWriterThread extends Thread {
        @Override
        public void run() {
            log.info("MetricWriter thread started");
            try {
                while (!isInterrupted()) {
                    String message = inputBuffer.take();
                    write( message);
                }
            } catch (InterruptedException ex) {
                //done!
            }
            log.info("MetricWriter thread complete");
        }

        /** continuously push message until success */
        public void write(String message) throws InterruptedException {
            while ( !isInterrupted()) {
                try {
                    client.put(message);
                    totalOutgoing.incrementAndGet();
                    break;
                } catch (IOException ex) {
                    log.error("Exception writing to server", ex);
                    alive.set( false);
                }
            }
        }
    }

    /**
     * A thread to asynchronously read OpenTsdb responses
     */
    class MetricReaderThread extends Thread {
        @Override
        public void run() {
            log.info("MetricReader thread started");
            while (!isInterrupted()) {
                try {
                    String message = client.read();
                    if (message == null) {
                        alive.set( false);
                    } else {
                        log.error( "OpenTsdb has error: {}", message);
                        totalErrors.incrementAndGet();
                    }
                } catch (IOException ex) {
                    log.error("Exception reading from server", ex);
                    alive.set( false);
                }
            }
            log.info("MetricReader thread complete");
        }
    }

    @SuppressWarnings({"unused"})
    public OpenTsdbMetricService() {
    }

    public OpenTsdbMetricService(ConsumerAppConfiguration config) {
        this.config = config;
    }
}
