/*
 * ****************************************************************************
 *
 *  Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 *
 *  This content is made available according to terms specified in
 *  License.zenoss under the directory where your Zenoss product is installed.
 *
 * ***************************************************************************
 */
package org.zenoss.app.consumer.metric.impl;

import com.google.common.eventbus.EventBus;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.ExecutorService;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.TsdbWriterRegistry;
import org.zenoss.app.consumer.metric.data.Control;
import static org.mockito.Mockito.*;
/**
 *
 * @author cschellenger
 */
public class OpenTsdbExecutorServiceTest {
 
    ApplicationContext context;
    MetricServiceConfiguration config;
    EventBus eventBus;
    ExecutorService executorService;
    TsdbWriterRegistry registry;
    
    @Before
    public void setUp() {
        context = mock(ApplicationContext.class);
        config = new MetricServiceConfiguration();
        eventBus = mock(EventBus.class);
        executorService = mock(ExecutorService.class);
        registry = mock(TsdbWriterRegistry.class);
    }
    
    OpenTsdbExecutorService createService() {
        return new OpenTsdbExecutorService(context, config, eventBus, executorService, registry);
    }
    
    void nothingHappens() {
        verify(context, never()).getBean(TsdbWriter.class);
        verify(registry, never()).size();
    }
    
    @Test
    public void testStartAllWritersLow() {
        config.setTsdbWriterThreads(3);
        TsdbWriter writer = mock(TsdbWriter.class);
        when (registry.size()).thenReturn (0);
        when (context.getBean(TsdbWriter.class)).thenReturn (writer);
        
        OpenTsdbExecutorService service = createService();
        service.processControl(Control.lowCollision());
        
        verify (executorService, times(3)).submit(writer);
    }
    
    @Test
    public void testStartAllWritersHigh() {
        config.setTsdbWriterThreads(3);
        TsdbWriter writer = mock(TsdbWriter.class);
        when (registry.size()).thenReturn (0);
        when (context.getBean(TsdbWriter.class)).thenReturn (writer);
        
        OpenTsdbExecutorService service = createService();
        service.processControl(Control.highCollision());
        
        verify (executorService, times(3)).submit(writer);
    }
    
    @Test
    public void testStartSomeWritersLow() {
        config.setTsdbWriterThreads(3);
        TsdbWriter writer = mock(TsdbWriter.class);
        when (registry.size()).thenReturn (1);
        when (context.getBean(TsdbWriter.class)).thenReturn (writer);
        
        OpenTsdbExecutorService service = createService();
        service.processControl(Control.lowCollision());
        
        verify (executorService, times(2)).submit(writer);
    }
    
    @Test
    public void testStartSomeWritersHigh() {
        config.setTsdbWriterThreads(3);
        TsdbWriter writer = mock(TsdbWriter.class);
        when (registry.size()).thenReturn (1);
        when (context.getBean(TsdbWriter.class)).thenReturn (writer);
        
        OpenTsdbExecutorService service = createService();
        service.processControl(Control.highCollision());
        
        verify (executorService, times(2)).submit(writer);
    }
    
    @Test
    public void testRunTwiceFast() {
        config.setTsdbWriterThreads(3);
        TsdbWriter writer = mock(TsdbWriter.class);
        when (registry.size()).thenReturn (1);
        when (context.getBean(TsdbWriter.class)).thenReturn (writer);
        
        OpenTsdbExecutorService service = createService();
        service.processControl(Control.lowCollision());
        service.processControl(Control.lowCollision());
        
        verify (executorService, times(2)).submit(writer);
    }
    
    @Test
    public void testRunTwiceFastLowHigh() {
        config.setTsdbWriterThreads(3);
        TsdbWriter writer = mock(TsdbWriter.class);
        when (registry.size()).thenReturn (1);
        when (context.getBean(TsdbWriter.class)).thenReturn (writer);
        
        OpenTsdbExecutorService service = createService();
        service.processControl(Control.lowCollision());
        service.processControl(Control.highCollision());
        
        verify (executorService, times(2)).submit(writer);
    }
    
    @Test
    public void testStartNoWritersLow() {
        config.setTsdbWriterThreads(3);
        TsdbWriter writer = mock(TsdbWriter.class);
        when (registry.size()).thenReturn (3);
        when (context.getBean(TsdbWriter.class)).thenReturn (writer);
        
        OpenTsdbExecutorService service = createService();
        service.processControl(Control.lowCollision());
        
        verify (executorService, times(0)).submit(writer);
    }
    
    @Test
    public void testStartNoWritersHigh() {
        config.setTsdbWriterThreads(3);
        TsdbWriter writer = mock(TsdbWriter.class);
        when (registry.size()).thenReturn (3);
        when (context.getBean(TsdbWriter.class)).thenReturn (writer);
        
        OpenTsdbExecutorService service = createService();
        service.processControl(Control.highCollision());
        
        verify (executorService, times(0)).submit(writer);
    }
    
    @Test
    public void testHandleDropped() {
        OpenTsdbExecutorService service = createService();
        service.processControl(Control.dropped(null));
        nothingHappens();
    }
    
    @Test
    public void testHandleError() {
        OpenTsdbExecutorService service = createService();
        service.processControl(Control.error(null));
        nothingHappens();
    }
    
    @Test
    public void testHandleMalformed() {
        OpenTsdbExecutorService service = createService();
        service.processControl(Control.malformedRequest(null));
        nothingHappens();
    }
    
    @Test
    public void testHandleOK() {
        OpenTsdbExecutorService service = createService();
        service.processControl(Control.ok());
        nothingHappens();
    }
    
}
