/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2017, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss distributed with this file.
 *
 * ***************************************************************************
 */
package org.zenoss.app.consumer.metric.zing;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.app.consumer.metric.ZingConfiguration;
import org.zenoss.app.consumer.metric.ZingSender;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.app.consumer.metric.zing.ZingQueue;
import org.zenoss.app.consumer.metric.zing.ZingWriter;
import org.zenoss.app.consumer.metric.zing.ZingWriterRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ZingWriterTest {

    static final Map<String, String> EMPTY_MAP = Collections.emptyMap();

    ZingConfiguration configuration;
    ExecutorService executor;
    ZingWriterRegistry registry;
    ZingQueue metricsQueue;
    ZingSender sender;

    @Before
    public void setUp() {
        configuration = new ZingConfiguration();
        configuration.setMaxIdleTime(1);
        metricsQueue = new ZingQueue();

        registry = mock(ZingWriterRegistry.class);
        sender = mock(ZingSender.class);
        executor = Executors.newSingleThreadExecutor();

    }

    @After
    public void tearDown() {
        metricsQueue.resetMetrics();
        executor.shutdownNow();
    }

    @Test
    public void testCancel() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);
        ZingQueue mq = mock(ZingQueue.class);

        configuration.setMaxIdleTime(0); // Never quit due to lack of work
        org.zenoss.app.consumer.metric.zing.ZingWriter writer = new ZingWriter(configuration, registry, mq, sender);

        Future<?> future = executor.submit(writer);
        boolean writerStarted = false;
        for (int tries = 0; tries < 50 && !writerStarted; tries++) {
            writerStarted = writer.isRunning();
            Thread.sleep(10);
        }
        if (!writerStarted) {
            fail("Writer could not be started");
        }

        writer.cancel();
        future.get(1, TimeUnit.SECONDS);
        assertFalse(writer.isRunning());
    }

    @Test
    public void testInterrupt() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);
        ZingQueue mq = mock(ZingQueue.class);

        configuration.setMaxIdleTime(0); // Never quit due to lack of work
        ZingWriter writer = new ZingWriter(configuration, registry, mq, sender);

        executor.submit(writer);

        boolean writerStarted = false;
        for (int tries = 0; tries < 50 && !writerStarted; tries++) {
            writerStarted = writer.isRunning();
            Thread.sleep(10);
        }
        if (!writerStarted) {
            fail("Writer could not be started");
        }

        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.SECONDS);
        assertFalse(writer.isRunning());
    }

    @Test
    public void testSubmitSuccess() throws Exception {
        ZingWriter writer = new ZingWriter(configuration, registry, metricsQueue, sender);
        executor.submit(writer);

        boolean writerStarted = false;
        for (int tries = 0; tries < 50 && !writerStarted; tries++) {
            writerStarted = writer.isRunning();
            Thread.sleep(10);
        }
        if (!writerStarted) {
            fail("Writer could not be started");
        }

        final Metric metric = new Metric("metric", 0, 0);
        Collection<Metric> batch = Lists.newArrayList(metric);

        metricsQueue.addAll(batch, "test");

        boolean writerIsRunning = writer.isRunning();
        for (int tries = 0; tries < 50 && writerIsRunning; tries++) {
            writerIsRunning = writer.isRunning();
            Thread.sleep(10);
        }
        if (writerIsRunning) {
            fail("Writer did not stop after all work completed");
        }

        assertEquals(0, metricsQueue.size());
        assertEquals(0, metricsQueue.getTotalErrors());
        assertEquals(1, metricsQueue.getTotalIncoming());
        assertEquals(1, metricsQueue.getTotalOutgoing());
        assertEquals(0, metricsQueue.getTotalInFlight());
        assertEquals(0, metricsQueue.getTotalLost());
    }

    @Test
    public void testSubmitStopsAfterSendFailure() throws Exception {
        ZingWriter writer = new ZingWriter(configuration, registry, metricsQueue, sender);
        executor.submit(writer);

        boolean writerStarted = false;
        for (int tries = 0; tries < 50 && !writerStarted; tries++) {
            writerStarted = writer.isRunning();
            Thread.sleep(10);
        }
        if (!writerStarted) {
            fail("Writer could not be started");
        }

        final Metric metric = new Metric("metric", 0, 0);
        Collection<Metric> batch = Lists.newArrayList(metric);

        RuntimeException e = new RuntimeException("mock send fail");
        doThrow(e).when(sender).send(batch);

        metricsQueue.addAll(batch, "test");

        boolean writerIsRunning = writer.isRunning();
        for (int tries = 0; tries < 50 && writerIsRunning; tries++) {
            writerIsRunning = writer.isRunning();
            Thread.sleep(10);
        }
        if (writerIsRunning) {
            fail("Writer did not stop after send failure");
        }

        assertEquals(0, metricsQueue.size());
        assertEquals(1, metricsQueue.getTotalErrors());
        assertEquals(1, metricsQueue.getTotalIncoming());
        assertEquals(0, metricsQueue.getTotalOutgoing());
        assertEquals(0, metricsQueue.getTotalInFlight());
        assertEquals(1, metricsQueue.getTotalLost());
    }

}
