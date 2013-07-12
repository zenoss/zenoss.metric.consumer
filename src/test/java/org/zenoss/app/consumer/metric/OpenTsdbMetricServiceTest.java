package org.zenoss.app.consumer.metric;

import com.google.common.collect.Lists;
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
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class OpenTsdbMetricServiceTest {

    ConsumerAppConfiguration config;

    EventBus eventBus;

    OpenTsdbExecutorService executorService;


    @Before
    public void setUp() {
        executorService = mock(OpenTsdbExecutorService.class);
        eventBus = mock(EventBus.class);
        config = new ConsumerAppConfiguration();
    }

    @Test
    public void testStartStop() throws Exception {
        OpenTsdbMetricService service = new OpenTsdbMetricService(config, executorService, eventBus);
        service.start();
        service.stop();
        verify(executorService, times(1)).stop();
    }

    @Test
    public void testPushHandlesNull() throws Exception {
        OpenTsdbMetricService service = new OpenTsdbMetricService(config, executorService, eventBus);
        assertEquals(Control.malformedRequest("metrics not nullable"), service.push(null));
    }

    @Test
    public void testPush() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics  = new Metric[] {metric};
        OpenTsdbMetricService service = new OpenTsdbMetricService(config, executorService, eventBus);
        assertEquals(Control.ok(), service.push(metrics));
        verify(executorService, times(1)).submit(Lists.newArrayList( metric));
    }

    @Test
    public void testPushSubmitsTwice() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = {metric, metric, metric, metric, metric, metric};
        config.getMetricServiceConfiguration().setJobSize(5);
        OpenTsdbMetricService service = new OpenTsdbMetricService(config, executorService, eventBus);
        assertEquals(Control.ok(), service.push(metrics));

        verify(executorService, times(1)).submit(Lists.newArrayList( metric, metric, metric, metric, metric));
        verify(executorService, times(1)).submit(Lists.newArrayList(metric));
    }

    @Test
    public void testPushWithOverflow() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = {metric};
        config.getMetricServiceConfiguration().setJobSize(Integer.MAX_VALUE);
        OpenTsdbMetricService service = new OpenTsdbMetricService(config, executorService, eventBus);
        assertEquals(Control.ok(), service.push(metrics));
        verify(executorService, times(1)).submit(Lists.newArrayList( metric));
    }

    @Test
    public void testPushCollidesLow() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = {metric, metric};
        config.getMetricServiceConfiguration().setLowCollisionMark(1);
        OpenTsdbMetricService service = new OpenTsdbMetricService(config, executorService, eventBus);
        assertEquals(Control.ok(), service.push(metrics));
        verify(executorService, times(1)).submit(Lists.newArrayList(metric, metric));
        verify(eventBus, times(1)).post(Control.lowCollision());
    }

    @Test
    public void testPushCollidesHigh() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = {metric, metric};
        config.getMetricServiceConfiguration().setHighCollisionMark(2);
        config.getMetricServiceConfiguration().setLowCollisionMark(1);
        OpenTsdbMetricService service = new OpenTsdbMetricService(config, executorService, eventBus);
        assertEquals(Control.dropped("collision detected"), service.push(metrics));
        verify(executorService, never()).submit(anyListOf(Metric.class));
        verify(eventBus, times(1)).post(Control.highCollision());
    }
}
