package org.zenoss.app.consumer.metric.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.lib.tsdb.OpenTsdbClient;
import org.zenoss.lib.tsdb.OpenTsdbClientPool;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.*;

import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.TsdbWriter;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class OpenTsdbWriterTest {

    MetricServiceConfiguration configuration;
    ExecutorService executor;
    MetricsQueue metricsQueue;
    OpenTsdbClient client;
    OpenTsdbClient badClient;
    OpenTsdbClient goodClient;
    OpenTsdbClientPool clientPool;
    OpenTsdbWriterRegistry registry;

    @Before
    public void setUp() {
        configuration = new MetricServiceConfiguration();
        configuration.setMaxIdleTime(0);
        configuration.setSleepWhenEmpty(1);
        metricsQueue = new MetricsQueue();
        client = mock(OpenTsdbClient.class);
        badClient = mock(OpenTsdbClient.class);
        goodClient = mock(OpenTsdbClient.class);
        clientPool = mock(OpenTsdbClientPool.class);
        registry = mock(OpenTsdbWriterRegistry.class);
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
        
        when (mq.poll(anyInt())).thenReturn(Collections.singleton(metric));
        when (clientPool.borrowObject()).thenReturn(client);
        
        TsdbWriter writer = new OpenTsdbWriter(configuration, registry, clientPool, mq);
        
        Future<?> future = executor.submit(writer);
        while (!writer.isRunning()) {
            Thread.sleep(10); // Wait until the thread has started
        }
        writer.cancel();
        future.get();
        assertFalse (writer.isRunning());
        
        verify(client, never()).close();
    }
    
    @Test
    public void testInterrupt() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);
        MetricsQueue mq = mock(MetricsQueue.class);
        
        when (mq.poll(anyInt())).thenReturn(Collections.singleton(metric));
        when (clientPool.borrowObject()).thenReturn(client);
        
        TsdbWriter writer = new OpenTsdbWriter(configuration, registry, clientPool, mq);
        
        executor.submit(writer);
        while (!writer.isRunning()) {
            Thread.sleep(10); // Wait until the thread has started
        }
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.SECONDS);
        assertFalse (writer.isRunning());
        
        verify(client, never()).close();
    }

    @Test
    public void testSubmitSuccess() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);

        when(clientPool.borrowObject()).thenReturn(client);
        
        metricsQueue.addAll(Collections.singleton(metric));
        executeWriter();
        
        String message = OpenTsdbClient.toPutMessage(metric.getMetric(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(client, times(1)).put(message);
        verify(clientPool, times(1)).returnObject(client);
        verify(client, never()).close();

        assertEquals (0, metricsQueue.getTotalErrors());
        assertEquals (1, metricsQueue.getTotalIncoming());
        assertEquals (1, metricsQueue.getTotalOutgoing());
        assertEquals (0, metricsQueue.getTotalInFlight());
    }

    @Test
    public void testSubmitSuccessAfterWriteException() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);

        when(clientPool.borrowObject()).thenReturn(badClient, goodClient);
        doThrow(new IOException()).when(badClient).put(anyString());
        
        metricsQueue.addAll(Collections.singleton(metric));
        executeWriter();

        String message = OpenTsdbClient.toPutMessage(metric.getMetric(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(badClient, times(1)).put(message);
        verify(goodClient, times(1)).put(message);
        verify(goodClient, times(1)).flush();
        verify(goodClient, never()).close();
        verify(clientPool, times(1)).returnObject(goodClient);
        verify(badClient, times(1)).close();

        assertEquals (0, metricsQueue.getTotalErrors());
        assertEquals (1, metricsQueue.getTotalIncoming());
        assertEquals (1, metricsQueue.getTotalOutgoing());
        assertEquals (0, metricsQueue.getTotalInFlight());
    }

    @Ignore // No longer reading with every write
    @Test
    public void testSubmitKillsClientAfterReadException() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);
        metricsQueue.addAll(Collections.singleton(metric));

        when(clientPool.borrowObject()).thenReturn(client);
        when(client.read()).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                throw new IOException();
            }
        });
        executeWriter();
        
        String message = OpenTsdbClient.toPutMessage(metric.getMetric(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(client, times(1)).put(message);
        verify(client, times(1)).read();
        verify(client, times(1)).flush();
        verify(client, times(1)).close();
        verify(clientPool, times(1)).returnObject(client);

        assertEquals (0, metricsQueue.getTotalErrors());
        assertEquals (1, metricsQueue.getTotalIncoming());
        assertEquals (1, metricsQueue.getTotalOutgoing());
        assertEquals (0, metricsQueue.getTotalInFlight());
    }

    @Test
    public void testSubmitSuccessWithErrorResponse() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);

        when(clientPool.clearErrorCount()).thenReturn(3, 0);
        when(clientPool.borrowObject()).thenReturn(client);
        
        metricsQueue.addAll(Collections.singleton(metric));
        executeWriter();

        String message = OpenTsdbClient.toPutMessage(metric.getMetric(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(client, times(1)).put(message);
        verify(client, never()).read();
        verify(client, times(1)).flush();
        verify(client, never()).close();
        verify(clientPool, times(1)).returnObject(client);

        assertEquals (3, metricsQueue.getTotalErrors());
        assertEquals (1, metricsQueue.getTotalIncoming());
        assertEquals (1, metricsQueue.getTotalOutgoing());
        assertEquals (0, metricsQueue.getTotalInFlight());
    }

    private void executeWriter() throws Exception {
        TsdbWriter writer = new OpenTsdbWriter(configuration, registry, clientPool, metricsQueue);
        Future<?> future = executor.submit(writer);
        future.get();
    }
}
