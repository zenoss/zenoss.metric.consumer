package org.zenoss.app.consumer.metric;

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

import static org.mockito.Mockito.*;

public class OpenTsdbExecutorServiceTest {

    MetricServiceConfiguration configuration;

    ExecutorService executorService;

    OpenTsdbClient client;

    OpenTsdbClient badClient;

    OpenTsdbClient goodClient;

    OpenTsdbClientPool clientPool;

    OpenTsdbMetricService metricService;

    @Before
    public void setUp() {
        configuration = new MetricServiceConfiguration();
        client = mock(OpenTsdbClient.class);
        badClient = mock(OpenTsdbClient.class);
        goodClient = mock(OpenTsdbClient.class);
        clientPool = mock(OpenTsdbClientPool.class);
        executorService = mock(ExecutorService.class);
        metricService = mock(OpenTsdbMetricService.class);
    }

    @Test
    public void testStop() throws Exception {
        configuration.setTerminationTimeout(10);
        new OpenTsdbExecutorService(configuration, executorService, clientPool).stop();
        verify(executorService, times(1)).shutdownNow();
        verify(executorService, times(1)).awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubmitThrowsIllegalArgumentException() throws Exception {
        OpenTsdbExecutorService service = new OpenTsdbExecutorService(configuration, executorService, clientPool);
        service.submit(null, null, -1, -1);
    }

    @Test
    public void testSubmitSuccess() throws Exception {
        final OpenTsdbExecutorService service = new OpenTsdbExecutorService(configuration, executorService, clientPool);
        final Metric metric = new Metric("metric", 0, 0);
        final Metric[] metrics = new Metric[]{metric};

        when(clientPool.get()).thenReturn(client);
        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((Runnable) invocation.getArguments()[0]).run();
                return null;
            }
        }).when(executorService).execute(any(Runnable.class));
        service.submit(metricService, metrics, 0, 1);

        String message = OpenTsdbClient.toPutMessage(metric.getName(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(client, times(1)).put(message);
        verify(metricService, times(0)).incrementTotalError(anyInt());
        verify(metricService, times(1)).incrementTotalProcessed(1);
        verify(clientPool, times(1)).put(client);
    }

    @Test
    public void testSubmitSuccessAfterWriteException() throws Exception {
        final OpenTsdbExecutorService service = new OpenTsdbExecutorService(configuration, executorService, clientPool);
        final Metric metric = new Metric("metric", 0, 0);
        final Metric[] metrics = new Metric[]{metric};

        when(clientPool.get()).thenReturn(badClient, goodClient);
        doThrow(new IOException()).when(badClient).put(anyString());
        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((Runnable) invocation.getArguments()[0]).run();
                return null;
            }
        }).when(executorService).execute(any(Runnable.class));
        service.submit(metricService, metrics, 0, 1);

        String message = OpenTsdbClient.toPutMessage(metric.getName(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(badClient, times(1)).put(message);
        verify(goodClient, times(1)).put(message);
        verify(goodClient, times(1)).flush();
        verify(metricService, times(0)).incrementTotalError(anyInt());
        verify(metricService, times(1)).incrementTotalProcessed(1);
        verify(clientPool, times(1)).put(goodClient);
        verify(clientPool, times(1)).kill(badClient);
    }

    @Test
    public void testSubmitKillsClientAfterReadException() throws Exception {
        final OpenTsdbExecutorService service = new OpenTsdbExecutorService(configuration, executorService, clientPool);
        final Metric metric = new Metric("metric", 0, 0);
        final Metric[] metrics = new Metric[]{metric};

        when(client.read()).thenThrow(new IOException());
        when(clientPool.get()).thenReturn(client);
        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((Runnable) invocation.getArguments()[0]).run();
                return null;
            }
        }).when(executorService).execute(any(Runnable.class));
        service.submit(metricService, metrics, 0, 1);

        String message = OpenTsdbClient.toPutMessage(metric.getName(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(client, times(1)).put(message);
        verify(client, times(1)).read();
        verify(client, times(1)).flush();
        verify(metricService, times(0)).incrementTotalError(anyInt());
        verify(metricService, times(1)).incrementTotalProcessed(1);
        verify(clientPool, times(1)).kill(client);
        verify(clientPool, times(0)).put(client);
    }

    @Test
    public void testSubmitSuccessWithErrorResponse() throws Exception {
        final OpenTsdbExecutorService service = new OpenTsdbExecutorService(configuration, executorService, clientPool);
        final Metric metric = new Metric("metric", 0, 0);
        final Metric[] metrics = new Metric[]{metric};

        when(client.read()).thenReturn ("an opentsdb error message\n");
        when(clientPool.get()).thenReturn(client);
        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((Runnable) invocation.getArguments()[0]).run();
                return null;
            }
        }).when(executorService).execute(any(Runnable.class));
        service.submit(metricService, metrics, 0, 1);

        String message = OpenTsdbClient.toPutMessage(metric.getName(), metric.getTimestamp(), metric.getValue(), metric.getTags());
        verify(client, times(1)).put(message);
        verify(client, times(1)).read();
        verify(client, times(1)).flush();
        verify(metricService, times(1)).incrementTotalError(1);
        verify(metricService, times(1)).incrementTotalProcessed(1);
        verify(clientPool, times(0)).kill(client);
        verify(clientPool, times(1)).put(client);
    }
}
