package org.zenoss.app.consumer.metric.impl;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import org.junit.Before;
import org.junit.Test;

import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class OpenTsdbMetricServiceTest {

    MetricServiceConfiguration config;
    EventBus eventBus;
    MetricsQueue metricsQueue;

    @Before
    public void setUp() {
        eventBus = mock(EventBus.class);
        config = new MetricServiceConfiguration();
        metricsQueue = mock(MetricsQueue.class);
    }
    
    OpenTsdbMetricService newService() {
        return new OpenTsdbMetricService(config, eventBus, metricsQueue);
    }

    @Test
    public void testPushHandlesNull() throws Exception {
        OpenTsdbMetricService service = newService();
        assertEquals(Control.malformedRequest ("metrics not nullable"), service.push(null));
    }

    @Test
    public void testPush() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics  = new Metric[] {metric};
        OpenTsdbMetricService service = newService();
        assertEquals(Control.ok(), service.push(metrics));
        verify(metricsQueue, times(1)).addAll (Lists.newArrayList (metric));
    }

    @Test
    public void testPushWithOverflow() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = {metric};
        config.setJobSize(Integer.MAX_VALUE);
        OpenTsdbMetricService service = newService();
        assertEquals(Control.ok(), service.push(metrics));
        verify(metricsQueue, times(1)).addAll (Lists.newArrayList (metric));
    }

    @Test
    public void testPushCollidesLow() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = {metric, metric};
        config.setLowCollisionMark(1);
        
        when(metricsQueue.getTotalInFlight()).thenReturn(2L);
        
        OpenTsdbMetricService service = newService();
        assertEquals(Control.ok(), service.push(metrics));
        
        verify(metricsQueue, times(1)).addAll(Lists.newArrayList(metric, metric));
        verify(eventBus, times(1)).post(Control.lowCollision());
    }

    @Test
    public void testPushCollidesHigh() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        Metric[] metrics = {metric, metric};
        config.setHighCollisionMark(2);
        config.setLowCollisionMark(1);
        when(metricsQueue.getTotalInFlight()).thenReturn(2L);
        
        OpenTsdbMetricService service = newService();
        assertEquals(Control.dropped("collision detected"), service.push(metrics));
        
        verify(metricsQueue, never()).addAll(anyListOf(Metric.class));
        verify(eventBus, times(1)).post(Control.highCollision());
    }
}
