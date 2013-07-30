package org.zenoss.app.consumer.metric.impl;

import org.junit.After;
import org.junit.Before;
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
    OpenTsdbWriter writer;

    @Before
    public void setUp() {
        configuration = new MetricServiceConfiguration();
        metricsQueue = new MetricsQueue();
        client = mock(OpenTsdbClient.class);
        badClient = mock(OpenTsdbClient.class);
        goodClient = mock(OpenTsdbClient.class);
        clientPool = mock(OpenTsdbClientPool.class);
        executor = Executors.newSingleThreadExecutor();
        writer = new OpenTsdbWriter(configuration, clientPool, metricsQueue);
    }
    
    @After
    public void tearDown() {
        metricsQueue.resetMetrics();
        executor.shutdownNow();
    }

    @Test
    public void testCancel() throws Exception {
        Future<?> future = executor.submit(writer);
        while (!writer.isRunning()) {
            Thread.sleep(10); // Wait until the thread has started
        }
        writer.cancel();
        future.get();
        assertFalse (writer.isRunning());
    }
    
    @Test
    public void testInterrupt() throws Exception {
        executor.submit(writer);
        while (!writer.isRunning()) {
            Thread.sleep(10); // Wait until the thread has started
        }
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.SECONDS);
        assertFalse (writer.isRunning());
    }

    @Test
    public void testSubmitSuccess() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);

        when(clientPool.borrowObject()).thenReturn(client);
        
        metricsQueue.addAll(Collections.singleton(metric));
        executeWriterOnce(client);
        
        String message = OpenTsdbClient.toPutMessage(metric.getMetric(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(client, times(1)).put(message);
        verify(clientPool, times(1)).returnObject(client);

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
        executeWriterOnce(goodClient);

        String message = OpenTsdbClient.toPutMessage(metric.getMetric(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(badClient, times(1)).put(message);
        verify(goodClient, times(1)).put(message);
        verify(goodClient, times(1)).flush();
        verify(clientPool, times(1)).returnObject(goodClient);
        verify(badClient, times(1)).close();

        assertEquals (0, metricsQueue.getTotalErrors());
        assertEquals (1, metricsQueue.getTotalIncoming());
        assertEquals (1, metricsQueue.getTotalOutgoing());
        assertEquals (0, metricsQueue.getTotalInFlight());
    }

    @Test
    public void testSubmitKillsClientAfterReadException() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);

        when(client.read()).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                writer.cancel(); // Stop thread after throwing this exception
                throw new IOException();
            }
        });
        when(clientPool.borrowObject()).thenReturn(client);
        metricsQueue.addAll(Collections.singleton(metric));
        Future<?> future = executor.submit(writer);
        future.get();
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

        when(client.read()).thenReturn("an opentsdb error message\n");
        when(clientPool.borrowObject()).thenReturn(client);
        
        metricsQueue.addAll(Collections.singleton(metric));
        executeWriterOnce(client);

        String message = OpenTsdbClient.toPutMessage(metric.getMetric(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(client, times(1)).put(message);
        verify(client, times(1)).read();
        verify(client, times(1)).flush();
        verify(client, times(0)).close();
        verify(clientPool, times(1)).returnObject(client);

        assertEquals (1, metricsQueue.getTotalErrors());
        assertEquals (1, metricsQueue.getTotalIncoming());
        assertEquals (1, metricsQueue.getTotalOutgoing());
        assertEquals (0, metricsQueue.getTotalInFlight());
    }

    /*
     * Executes the writer under test and marks the thread as canceled after 
     * the specified client is returned to the poool. Note that the write will
     * only be returned to the pool if it is borrowed in the first place; this
     * only happens when there is data to be written.
     */
    private void executeWriterOnce(OpenTsdbClient someClient) throws Exception {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                writer.cancel();
                return null;
            }
        }).when(clientPool).returnObject(someClient);
        Future<?> future = executor.submit(writer);
        future.get();
    }
}
