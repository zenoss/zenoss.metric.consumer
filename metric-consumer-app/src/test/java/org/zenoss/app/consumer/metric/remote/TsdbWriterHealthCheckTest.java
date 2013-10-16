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

import org.zenoss.app.consumer.metric.TsdbMetricsQueue;
import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.TsdbWriterRegistry;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
/**
 *
 * @author cschellenger
 */
public class TsdbWriterHealthCheckTest {
    
    
    @Test
    public void testCheckHealthy() {
        TsdbWriterRegistry registry = mock(TsdbWriterRegistry.class);
        TsdbMetricsQueue queue = mock(TsdbMetricsQueue.class);
        when (registry.size()).thenReturn (1);
        when (queue.getTotalInFlight()).thenReturn (1L);

        TsdbWriterHealthCheck healthCheck = new TsdbWriterHealthCheck(registry, queue);
        assertTrue(healthCheck.check().isHealthy());
    }
    
    @Test
    public void testCheckUnhealthyNoWriters() {
        TsdbWriterRegistry registry = mock(TsdbWriterRegistry.class);
        TsdbMetricsQueue queue = mock(TsdbMetricsQueue.class);
        when (registry.size()).thenReturn (0);
        when (queue.getTotalInFlight()).thenReturn (1L);

        TsdbWriterHealthCheck healthCheck = new TsdbWriterHealthCheck(registry, queue);
        assertFalse(healthCheck.check().isHealthy());
    }
    
    @Test
    public void testCheckUnhealthyNoMetrics() {
        TsdbWriterRegistry registry = mock(TsdbWriterRegistry.class);
        TsdbMetricsQueue queue = mock(TsdbMetricsQueue.class);
        when (registry.size()).thenReturn (1);
        when (queue.getTotalInFlight()).thenReturn (0L);

        TsdbWriterHealthCheck healthCheck = new TsdbWriterHealthCheck(registry, queue);
        assertFalse(healthCheck.check().isHealthy());
    }
}
