package org.zenoss.app.consumer.metric;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.zenoss.app.consumer.ConsumerAppConfiguration;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.lib.tsdb.OpenTsdbClient;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class OpenTsdbMetricServiceTest {

    ConsumerAppConfiguration config;

    OpenTsdbExecutorService executorService;

    OpenTsdbMetricService service;

    @Before
    public void setUp() {
        executorService = mock(OpenTsdbExecutorService.class);
        config = new ConsumerAppConfiguration();
        service = new OpenTsdbMetricService(config, executorService);
    }

    @Test
    public void testStartStop() throws Exception {
        service.start();
        service.stop();
        verify(executorService, times(1)).stop();
    }

    @Test
    public void testPushHandlesNull() throws Exception {
        assertEquals( new Control(), service.push( null));
    }

    @Test
    public void testPush() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = { metric};
        assertEquals( new Control(), service.push( metrics));
        verify( executorService, times(1)).submit(service, metrics, 0, 1);
    }

    @Test
    public void testPushTwice() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = { metric, metric, metric, metric, metric, metric};
        config.getMetricServiceConfiguration().setJobSize( 5);
        assertEquals( new Control(), service.push( metrics));
        verify( executorService, times(1)).submit(service, metrics, 0, 5);
        verify( executorService, times(1)).submit(service, metrics, 5, 6);
    }

    @Test
    public void testPushWithOverflow() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = { metric};
        config.getMetricServiceConfiguration().setJobSize( Integer.MAX_VALUE);
        assertEquals( new Control(), service.push( metrics));
        verify( executorService, times(1)).submit(service, metrics, 0, 1);
    }
}
