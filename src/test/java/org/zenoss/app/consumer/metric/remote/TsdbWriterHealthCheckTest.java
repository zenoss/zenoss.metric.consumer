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
package org.zenoss.app.consumer.metric.remote;

import org.junit.Test;

import java.util.Collections;

import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.TsdbWriterFactory;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
/**
 *
 * @author cschellenger
 */
public class TsdbWriterHealthCheckTest {
    
    
    @Test
    public void testCheckHealthy() {
        TsdbWriterFactory factory = mock(TsdbWriterFactory.class);
        TsdbWriter writer = mock(TsdbWriter.class);
        when(factory.getCreatedWriters()).thenReturn(Collections.singleton(writer));
        when(writer.isRunning()).thenReturn(Boolean.TRUE);
        
        TsdbWriterHealthCheck healthCheck = new TsdbWriterHealthCheck(factory);
        assertTrue(healthCheck.check().isHealthy());
    }
    
    @Test
    public void testCheckUnhealthyStopped() {
        TsdbWriterFactory factory = mock(TsdbWriterFactory.class);
        TsdbWriter writer = mock(TsdbWriter.class);
        when(factory.getCreatedWriters()).thenReturn(Collections.singleton(writer));
        when(writer.isRunning()).thenReturn(Boolean.FALSE);
        
        TsdbWriterHealthCheck healthCheck = new TsdbWriterHealthCheck(factory);
        assertFalse(healthCheck.check().isHealthy());
    }
    
    @Test
    public void testCheckUnhealthyEmpty() {
        TsdbWriterFactory factory = mock(TsdbWriterFactory.class);
        when(factory.getCreatedWriters()).thenReturn(Collections.<TsdbWriter>emptyList());
        
        TsdbWriterHealthCheck healthCheck = new TsdbWriterHealthCheck(factory);
        assertFalse(healthCheck.check().isHealthy());
    }
}
