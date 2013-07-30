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

import java.util.Collections;
import org.zenoss.app.consumer.metric.MetricService;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.TsdbWriterFactory;
import org.zenoss.app.consumer.metric.data.Control;
import static org.mockito.Mockito.*;

/**
 *
 * @author cschellenger
 */
public class OpenTsdbWriterThreadDoctorTest {
    
    void nothingHappens() {
        verify(writerFactory, never()).getCreatedWriters();
        verify(metricService, never()).startWriter();
    }
    
    @Test
    public void testHandleDropped() {
        OpenTsdbWriterThreadDoctor dr = newDoctor();
        dr.handle(Control.dropped(null));
        nothingHappens();
    }
    
    @Test
    public void testHandleError() {
        OpenTsdbWriterThreadDoctor dr = newDoctor();
        dr.handle(Control.error(null));
        nothingHappens();
    }
    
    @Test
    public void testHandleMalformed() {
        OpenTsdbWriterThreadDoctor dr = newDoctor();
        dr.handle(Control.malformedRequest(null));
        nothingHappens();
    }
    
    @Test
    public void testHandleOK() {
        OpenTsdbWriterThreadDoctor dr = newDoctor();
        dr.handle(Control.ok());
        nothingHappens();
    }
    
    @Test
    public void testHandleHighCollisionRunningFine() {
        TsdbWriter writer = mock(TsdbWriter.class);
        
        when(writerFactory.getCreatedWriters()).thenReturn(Collections.singleton(writer));
        when(writer.isRunning()).thenReturn(Boolean.TRUE);
        
        OpenTsdbWriterThreadDoctor dr = newDoctor();
        dr.handle(Control.highCollision());
        
        verify(writerFactory, times(1)).getCreatedWriters();
        verify(metricService, never()).startWriter();
    }
    
    @Test
    public void testHandleHighCollisionStopped() {
        TsdbWriter writer = mock(TsdbWriter.class);
        
        when(writerFactory.getCreatedWriters()).thenReturn(Collections.singleton(writer));
        when(writer.isRunning()).thenReturn(Boolean.FALSE);
        
        OpenTsdbWriterThreadDoctor dr = newDoctor();
        dr.handle(Control.highCollision());
        
        verify(writerFactory, times(1)).getCreatedWriters();
        verify(metricService, times(1)).startWriter();
    }
    
    @Test
    public void testHandleLowCollisionRunningFine() {
        TsdbWriter writer = mock(TsdbWriter.class);
        
        when(writerFactory.getCreatedWriters()).thenReturn(Collections.singleton(writer));
        when(writer.isRunning()).thenReturn(Boolean.TRUE);
        
        OpenTsdbWriterThreadDoctor dr = newDoctor();
        dr.handle(Control.lowCollision());
        
        verify(writerFactory, times(1)).getCreatedWriters();
        verify(metricService, never()).startWriter();
    }
    
    @Test
    public void testHandleLowCollisionStopped() {
        TsdbWriter writer = mock(TsdbWriter.class);
        
        when(writerFactory.getCreatedWriters()).thenReturn(Collections.singleton(writer));
        when(writer.isRunning()).thenReturn(Boolean.FALSE);
        
        OpenTsdbWriterThreadDoctor dr = newDoctor();
        dr.handle(Control.lowCollision());
        
        verify(writerFactory, times(1)).getCreatedWriters();
        verify(metricService, times(1)).startWriter();
    }
    
    @Before
    public void setUp() {
        config = new MetricServiceConfiguration();
        writerFactory = mock(TsdbWriterFactory.class);
        metricService = mock(MetricService.class);
        eventBus = mock(EventBus.class);
    }
    
    OpenTsdbWriterThreadDoctor newDoctor() {
        return new OpenTsdbWriterThreadDoctor(config, metricService, writerFactory, eventBus);
    }
    
    MetricServiceConfiguration config;
    TsdbWriterFactory writerFactory;
    MetricService metricService;
    EventBus eventBus;
    
}
