package org.zenoss.app.consumer.metric;

import com.google.common.eventbus.EventBus;
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

    EventBus eventBus;

    OpenTsdbExecutorService executorService;

    OpenTsdbMetricService service;

    @Before
    public void setUp() {
        executorService = mock(OpenTsdbExecutorService.class);
        eventBus = mock(EventBus.class);
        config = new ConsumerAppConfiguration();
        service = new OpenTsdbMetricService(config, executorService, eventBus);
    }

    @Test
    public void testStartStop() throws Exception {
        service.start();
        service.stop();
        verify(executorService, times(1)).stop();
    }

    @Test
    public void testPushHandlesNull() throws Exception {
        assertEquals(Control.malformedRequest("metrics not nullable"), service.push(null));
    }

    @Test
    public void testPush() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = {metric};
        assertEquals(Control.ok(), service.push(metrics));
        verify(executorService, times(1)).submit(service, metrics, 0, 1);
    }

    @Test
    public void testPushSubmitsTwice() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = {metric, metric, metric, metric, metric, metric};
        config.getMetricServiceConfiguration().setJobSize(5);
        assertEquals(Control.ok(), service.push(metrics));
        verify(executorService, times(1)).submit(service, metrics, 0, 5);
        verify(executorService, times(1)).submit(service, metrics, 5, 6);
    }

    @Test
    public void testPushWithOverflow() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = {metric};
        config.getMetricServiceConfiguration().setJobSize(Integer.MAX_VALUE);
        assertEquals(Control.ok(), service.push(metrics));
        verify(executorService, times(1)).submit(service, metrics, 0, 1);
    }

    @Test
    public void testPushCollidesLow() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = {metric, metric};
        config.getMetricServiceConfiguration().setLowCollisionMark(1);
        assertEquals(Control.ok(), service.push(metrics));
        verify(executorService, times(1)).submit(service, metrics, 0, 2);
        verify(eventBus, times(1)).post( Control.lowCollision());
    }

    @Test
    public void testPushCollidesHigh() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = {metric, metric};
        config.getMetricServiceConfiguration().setHighCollisionMark(2);
        config.getMetricServiceConfiguration().setLowCollisionMark(1);
        assertEquals(Control.dropped("collision detected"), service.push(metrics));
        verify(executorService, never()).submit(any(OpenTsdbMetricService.class), any(Metric[].class), anyInt(), anyInt());
        verify(eventBus, times(1)).post( Control.highCollision());
    }
}
