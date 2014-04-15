package org.zenoss.app.consumer.metric.impl;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import org.junit.Before;
import org.junit.Test;

import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
        List<Metric> metrics  = Collections.singletonList(new Metric("name", 0, 0.0));
        OpenTsdbMetricService service = newService();
        assertEquals(Control.malformedRequest("metrics not nullable"), service.push(null, "test"));
        assertEquals(Control.malformedRequest("clientId not nullable"), service.push(metrics, null));
    }

    @Test
    public void testPush() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        List<Metric> metrics  = Collections.singletonList(metric);
        OpenTsdbMetricService service = newService();
        assertEquals(Control.ok(), service.push(metrics, "test"));
        verify(metricsQueue, times(1)).addAll(metrics, "test");
    }

    @Test
    public void testPushWithOverflow() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        List<Metric> metrics  = Collections.singletonList(metric);
        config.setJobSize(Integer.MAX_VALUE);
        OpenTsdbMetricService service = newService();
        assertEquals(Control.ok(), service.push(metrics, "test"));
        verify(metricsQueue, times(1)).addAll(metrics, "test");
    }

    @Test
    public void testPushCollidesLow() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        ArrayList<Metric> metrics = Lists.newArrayList(metric, metric);
        config.setLowCollisionMark(1);
        
        when(metricsQueue.getTotalInFlight()).thenReturn(2L);
        
        OpenTsdbMetricService service = newService();
        assertEquals(Control.ok(), service.push(metrics,"test"));
        
        verify(metricsQueue, times(1)).addAll(metrics, "test");
        verify(eventBus, times(1)).post(Control.lowCollision());
    }

    @Test
    public void testPushCollidesHigh() throws Exception {
        Metric metric = new Metric("name", 0, 0.0);
        List<Metric> metricList = Lists.newArrayList(metric, metric);
        config.setHighCollisionMark(3);
        config.setLowCollisionMark(1);
        config.setMaxClientWaitTime(1);
        when(metricsQueue.getTotalInFlight()).thenReturn(3L);
        
        OpenTsdbMetricService service = newService();
        assertEquals(Control.dropped("collision detected"), service.push(metricList,"test"));
        
        verify(metricsQueue, never()).addAll(metricList, "test");
        verify(eventBus, atLeastOnce()).post(Control.highCollision());
    }
}
