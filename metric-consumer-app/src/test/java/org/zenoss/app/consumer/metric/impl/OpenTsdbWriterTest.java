package org.zenoss.app.consumer.metric.impl;

import com.google.common.eventbus.EventBus;
import com.yammer.metrics.core.MetricName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.lib.tsdb.OpenTsdbClient;
import org.zenoss.lib.tsdb.OpenTsdbClientPool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class OpenTsdbWriterTest {

    static final Map<String, String> EMPTY_MAP = Collections.emptyMap();

    MetricServiceConfiguration configuration;
    ExecutorService executor;
    MetricsQueue metricsQueue;
    OpenTsdbClient client;
    OpenTsdbClient badClient;
    OpenTsdbClient goodClient;
    OpenTsdbClientPool clientPool;
    OpenTsdbWriterRegistry registry;
    EventBus eventBus;

    @Before
    public void setUp() {
        configuration = new MetricServiceConfiguration();
        configuration.setMaxIdleTime(1);
        metricsQueue = new WriterTestQueue();
        client = mock(OpenTsdbClient.class);
        badClient = mock(OpenTsdbClient.class);
        goodClient = mock(OpenTsdbClient.class);
        clientPool = mock(OpenTsdbClientPool.class);
        registry = mock(OpenTsdbWriterRegistry.class);
        eventBus = mock(EventBus.class);
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
        MetricsQueue mq = mock(MetricsQueue.class);

        when(mq.poll(anyInt(), eq(1L))).thenReturn(Collections.singleton(metric));
        when(clientPool.borrowObject()).thenReturn(client);

        configuration.setMaxIdleTime(0); // Never quit due to lack of work
        TsdbWriter writer = new OpenTsdbWriter(configuration, registry, clientPool, mq, eventBus);

        Future<?> future = executor.submit(writer);
        boolean writerStarted = false;
        for (int tries = 0; tries < 50; tries++) {
            writerStarted = writer.isRunning();
            Thread.sleep(10);
        }
        if (!writerStarted) {
            fail("Writer could not be started");
        }

        writer.cancel();
        future.get(1, TimeUnit.SECONDS);
        assertFalse(writer.isRunning());

        verify(client, never()).close();
    }

    @Test
    public void testInterrupt() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);
        MetricsQueue mq = mock(MetricsQueue.class);

        when(mq.poll(anyInt(), eq(1L))).thenReturn(Collections.singleton(metric));
        when(clientPool.borrowObject()).thenReturn(client);

        configuration.setMaxIdleTime(0); // Never quit due to lack of work
        TsdbWriter writer = new OpenTsdbWriter(configuration, registry, clientPool, mq, eventBus);

        executor.submit(writer);

        boolean writerStarted = false;
        for (int tries = 0; tries < 50; tries++) {
            writerStarted = writer.isRunning();
            Thread.sleep(10);
        }
        if (!writerStarted) {
            fail("Writer could not be started");
        }

        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.SECONDS);
        assertFalse(writer.isRunning());

        verify(client, never()).close();
    }

    @Test
    public void testSubmitSuccess() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);
        String message = OpenTsdbClient.toPutMessage("metric", 0, 0.0, EMPTY_MAP);

        when(clientPool.borrowObject()).thenReturn(client);

        metricsQueue.addAll(Collections.singleton(metric), "test");
        executeWriter();

        verify(client, times(1)).put(message);
        verify(clientPool, times(1)).returnObject(client);
        verify(client, never()).close();

        assertEquals(0, metricsQueue.getTotalErrors());
        assertEquals(1, metricsQueue.getTotalIncoming());
        assertEquals(1, metricsQueue.getTotalOutgoing());
        assertEquals(0, metricsQueue.getTotalInFlight());
    }

    @Test
    public void testSubmitSuccessAfterWriteException() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);
        String message = OpenTsdbClient.toPutMessage("metric", 0, 0.0, EMPTY_MAP);

        when(clientPool.borrowObject()).thenReturn(badClient, goodClient);
        doThrow(new IOException()).when(badClient).put(anyString());

        metricsQueue.addAll(Collections.singleton(metric), "test");
        configuration.setMaxIdleTime(100);
        executeWriter();

        verify(badClient, times(1)).put(message);
        verify(goodClient, times(1)).put(message);
        verify(goodClient, times(1)).flush();
        verify(goodClient, never()).close();
        verify(clientPool, times(1)).returnObject(goodClient);
        verify(badClient, times(1)).close();

        assertEquals(0, metricsQueue.getTotalErrors());
        assertEquals(1, metricsQueue.getTotalIncoming());
        assertEquals(1, metricsQueue.getTotalOutgoing());
        assertEquals(0, metricsQueue.getTotalInFlight());
    }

    @Test
    public void testSubmitSuccessWithErrorResponse() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);
        String message = OpenTsdbClient.toPutMessage("metric", 0, 0.0, EMPTY_MAP);

        when(clientPool.clearErrorCount()).thenReturn(3, 0);
        when(clientPool.borrowObject()).thenReturn(client);

        metricsQueue.addAll(Collections.singleton(metric), "test");
        executeWriter();

        verify(client, times(1)).put(message);
        verify(client, never()).read();
        verify(client, times(1)).flush();
        verify(client, never()).close();
        verify(clientPool, times(1)).returnObject(client);

        assertEquals(3, metricsQueue.getTotalErrors());
        assertEquals(1, metricsQueue.getTotalIncoming());
        assertEquals(1, metricsQueue.getTotalOutgoing());
        assertEquals(0, metricsQueue.getTotalInFlight());
    }

    @Test
    public void testSubmitHasCollision() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);
        String message = OpenTsdbClient.toPutMessage("metric", 0, 0.0, EMPTY_MAP);

        when(clientPool.clearErrorCount()).thenReturn(1, 0);
        when(clientPool.hasCollision()).thenReturn(true, false);
        when(clientPool.borrowObject()).thenReturn(client);

        metricsQueue.addAll(Collections.singleton(metric), "test");
        executeWriter();

        verify(client, times(1)).put(message);
        verify(client, never()).read();
        verify(client, times(1)).flush();
        verify(client, never()).close();
        //the object's returned twice
        verify(clientPool, times(2)).returnObject(client);

        assertEquals(1, metricsQueue.getTotalErrors());
        assertEquals(1, metricsQueue.getTotalIncoming());
        assertEquals(1, metricsQueue.getTotalOutgoing());
        assertEquals(0, metricsQueue.getTotalInFlight());
    }


    @Test
    public void testConvert() {

        long timestamp = 1000;
        double value = 1.2;
        Metric m = new Metric("testName", timestamp, value);
        String put = OpenTsdbWriter.convert(m);
        String expected = "put testName 1000 1.2\n";
        assertEquals(expected, put);

        HashMap<String, String> tags = new HashMap<>();
        m = new Metric("testName", timestamp, value, tags);
        put = OpenTsdbWriter.convert(m);
        expected = "put testName 1000 1.2\n";
        assertEquals(expected, put);

        tags.put("tagKey", "tagVal");
        m = new Metric("testName", timestamp, value, tags);
        put = OpenTsdbWriter.convert(m);
        expected = "put testName 1000 1.2 tagKey=tagVal\n";
        assertEquals(expected, put);

        //Test tag is sanitized
        tags.put("test key", "test value");
        m = new Metric("testName", timestamp, value, tags);
        put = OpenTsdbWriter.convert(m);
        expected = "put testName 1000 1.2 tagKey=tagVal test-key=test-value\n";
        assertEquals(expected, put);

    }

    @Test
    public void testSanitize() {
        String input = "hello_ [{]]THERE-=)(*&^%$#@!.";
        String output = OpenTsdbWriter.sanitize(input);
        assertEquals("hello_-----THERE------------.", output);
    }

    private void executeWriter() throws Exception {
        TsdbWriter writer = new OpenTsdbWriter(configuration, registry, clientPool, metricsQueue, eventBus);
        Future<?> future = executor.submit(writer);
        future.get();
    }

    /*
     * This inner class is necessary because the yammer metrics used internally
     * by MetricsQueue use global metrics that would otherwise conflict with
     * other test threads.
     */
    private static class WriterTestQueue extends MetricsQueue {

        @Override
        MetricName incomingMetricName() {
            return new MetricName(MetricsQueue.class, "totalIncomingTsdbWriter");
        }

        @Override
        MetricName outgoingMetricName() {
            return new MetricName(MetricsQueue.class, "totalOutgoingTsdbWriter");
        }

        @Override
        MetricName inFlightMetricName() {
            return new MetricName(MetricsQueue.class, "totalInFlightTsdbWriter");
        }

        @Override
        MetricName errorsMetricName() {
            return new MetricName(MetricsQueue.class, "totalErrorsTsdbWriter");
        }

    }
}
