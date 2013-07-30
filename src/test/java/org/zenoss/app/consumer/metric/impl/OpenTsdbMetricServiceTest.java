package org.zenoss.app.consumer.metric.impl;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.concurrent.ExecutorService;

import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.TsdbWriterFactory;
import org.zenoss.app.consumer.metric.data.Control;
import org.zenoss.app.consumer.metric.data.Metric;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class OpenTsdbMetricServiceTest {

    MetricServiceConfiguration config;
    ExecutorService executorService;
    EventBus eventBus;
    MetricsQueue metricsQueue;
    TsdbWriterFactory writerFactory;

    @Before
    public void setUp() {
        executorService = mock(ExecutorService.class);
        writerFactory = mock(TsdbWriterFactory.class);
        eventBus = mock(EventBus.class);
        config = new MetricServiceConfiguration();
        metricsQueue = mock(MetricsQueue.class);
    }
    
    OpenTsdbMetricService newService() {
        return new OpenTsdbMetricService(config, eventBus, executorService, metricsQueue, writerFactory);
    }

    @Test
    public void testStartStop() throws Exception {
        OpenTsdbMetricService service = newService();
        
        TsdbWriter writer = mock(TsdbWriter.class);
        when(writerFactory.createWriter()).thenReturn(writer);
        
        service.start();
        service.stop();
        
        verify(executorService, times(1)).submit(writer);
        verify(executorService, times(1)).shutdownNow();
    }
    
    @Test (expected = IllegalStateException.class)
    public void testStartWithNoWriters() throws Exception {
        config.setTsdbWriterThreads(0);
        OpenTsdbMetricService service = newService();
        
        service.start();
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
