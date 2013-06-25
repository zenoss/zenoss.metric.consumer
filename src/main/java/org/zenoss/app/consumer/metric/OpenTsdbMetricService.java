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

    AtomicLong totalIncoming  = new AtomicLong(0);

    AtomicLong totalOutgoing = new AtomicLong(0);

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
        } catch ( InterruptedException ex) {
        }

        //because opentsdb uses a buffered reader, the socket has to close to interrupt
        client.close();
        try {
            reader.join();
        } catch ( InterruptedException ex) {
        }
    }

    @Override
    public Control push(Metric metric) {
        if ( !isAlive()) {
            //connection's down, try another client
            return new Control();
        }

        //create put message
        String name = metric.getName();
        long timestamp = metric.getTimestamp();
        double value = metric.getValue();
        Map<String, String> tags = metric.getTags();
        String message = OpenTsdbSocketClient.toPutMessage(name, timestamp, value, tags);

        if (inputBuffer.offer(message)) {
            totalIncoming.incrementAndGet();
            //success
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
        //TODO understand how to test for "connectivity" which is different than socket.isConnected
        return true;
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
            log.info( "MetricWriter thread started");
            try {
                while (!isInterrupted()) {
                    String message = inputBuffer.take();
                    try {
                        client.put(message);
                        totalOutgoing.incrementAndGet();
                    } catch (IOException ex) {
                        log.error( "Exception writing to server", ex);
                        //TODO connection's probably closed, ergo reconnect
                    }
                }
            } catch (InterruptedException ex) {
                //done!
            }
            log.info( "MetricWriter thread complete");
        }
    }


    /**
     * A thread to asynchronously read OpenTsdb responses
     */
    class MetricReaderThread extends Thread {
        @Override
        public void run() {
            log.info( "MetricReader thread started");
            while (!isInterrupted()) {
                try {
                    String message = client.read();
                    if ( message == null) {
                        //TODO connection's closed, ergo reconnect
                    }
                } catch ( IOException ex) {
                    log.error( "Exception reading from server", ex);
                    //TODO connection's probably closed, ergo reconnect
                }
            }
            log.info( "MetricReader thread complete");
        }
    }

    @SuppressWarnings({"unused"})
    public OpenTsdbMetricService() {
    }

    public OpenTsdbMetricService(ConsumerAppConfiguration config) {
        this.config = config;
    }
}
