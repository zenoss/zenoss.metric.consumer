package org.zenoss.app.consumer.metric;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.lib.tsdb.OpenTsdbClient;
import org.zenoss.lib.tsdb.OpenTsdbClientPool;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class OpenTsdbExecutorServiceTest {

    MetricServiceConfiguration configuration;
    ExecutorService executorService;
    OpenTsdbClient client;
    OpenTsdbClient badClient;
    OpenTsdbClient goodClient;
    OpenTsdbClientPool clientPool;
    OpenTsdbExecutorService service;

    @Before
    public void setUp() {
        configuration = new MetricServiceConfiguration();
        client = mock(OpenTsdbClient.class);
        badClient = mock(OpenTsdbClient.class);
        goodClient = mock(OpenTsdbClient.class);
        clientPool = mock(OpenTsdbClientPool.class);
        executorService = mock(ExecutorService.class);
        service = new OpenTsdbExecutorService(configuration, executorService, clientPool);
    }
    
    @After
    public void tearDown() {
        service.resetMetrics();
    }

    @Test
    public void testStop() throws Exception {
        configuration.setTerminationTimeout(10);
        service.stop();
        verify(executorService, times(1)).shutdownNow();
        verify(executorService, times(1)).awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testSubmitThrowsIllegalArgumentException() throws Exception {
        service.submit(null);
    }

    @Test
    public void testSubmitSuccess() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);

        when(clientPool.get()).thenReturn(client);
        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((Runnable) invocation.getArguments()[0]).run();
                return null;
            }
        }).when(executorService).execute(any(Runnable.class));
        service.submit(Lists.newArrayList(metric));
        String message = OpenTsdbClient.toPutMessage(metric.getMetric(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(client, times(1)).put(message);
        verify(clientPool, times(1)).put(client);

        assertEquals( 0, service.getTotalErrors());
        assertEquals( 1, service.getTotalIncoming());
        assertEquals( 1, service.getTotalOutGoing());
        assertEquals( 0, service.getTotalInFlight());
    }

    @Test
    public void testSubmitSuccessAfterWriteException() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);

        when(clientPool.get()).thenReturn(badClient, goodClient);
        doThrow(new IOException()).when(badClient).put(anyString());
        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((Runnable) invocation.getArguments()[0]).run();
                return null;
            }
        }).when(executorService).execute(any(Runnable.class));
        service.submit(Lists.newArrayList(metric));

        String message = OpenTsdbClient.toPutMessage(metric.getMetric(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(badClient, times(1)).put(message);
        verify(goodClient, times(1)).put(message);
        verify(goodClient, times(1)).flush();
        verify(clientPool, times(1)).put(goodClient);
        verify(clientPool, times(1)).kill(badClient);

        assertEquals( 0, service.getTotalErrors());
        assertEquals( 1, service.getTotalIncoming());
        assertEquals( 1, service.getTotalOutGoing());
        assertEquals( 0, service.getTotalInFlight());
    }

    @Test
    public void testSubmitKillsClientAfterReadException() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);

        when(client.read()).thenThrow(new IOException());
        when(clientPool.get()).thenReturn(client);
        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((Runnable) invocation.getArguments()[0]).run();
                return null;
            }
        }).when(executorService).execute(any(Runnable.class));
        service.submit(Lists.newArrayList(metric));

        String message = OpenTsdbClient.toPutMessage(metric.getMetric(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(client, times(1)).put(message);
        verify(client, times(1)).read();
        verify(client, times(1)).flush();
        verify(clientPool, times(1)).kill(client);
        verify(clientPool, times(0)).put(client);

        assertEquals( 0, service.getTotalErrors());
        assertEquals( 1, service.getTotalIncoming());
        assertEquals( 1, service.getTotalOutGoing());
        assertEquals( 0, service.getTotalInFlight());
    }

    @Test
    public void testSubmitSuccessWithErrorResponse() throws Exception {
        final Metric metric = new Metric("metric", 0, 0);

        when(client.read()).thenReturn("an opentsdb error message\n");
        when(clientPool.get()).thenReturn(client);
        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((Runnable) invocation.getArguments()[0]).run();
                return null;
            }
        }).when(executorService).execute(any(Runnable.class));
        service.submit(Lists.newArrayList(metric));

        String message = OpenTsdbClient.toPutMessage(metric.getMetric(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(client, times(1)).put(message);
        verify(client, times(1)).read();
        verify(client, times(1)).flush();
        verify(clientPool, times(0)).kill(client);
        verify(clientPool, times(1)).put(client);

        assertEquals( 1, service.getTotalErrors());
        assertEquals( 1, service.getTotalIncoming());
        assertEquals( 1, service.getTotalOutGoing());
        assertEquals( 0, service.getTotalInFlight());
    }
}
