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

import com.google.common.collect.ImmutableMultimap;
import org.junit.Test;

import org.zenoss.app.consumer.metric.MetricService;
import static org.mockito.Mockito.*;
/**
 *
 * @author cschellenger
 */
public class StartWriterTaskTest {
    
    /*
     * Tests result with a parameter for threads
     */
    @Test
    public void testExecuteWithThreads() {
        MetricService metricService = mock(MetricService.class);
        
        StartWriterTask task = new StartWriterTask(metricService);
        task.execute(ImmutableMultimap.of("threads", "3"), null);
        
        verify(metricService, times(3)).startWriter();
    }
    
    /*
     * Tests result with no parameters
     */
    @Test
    public void testExecuteNoThreads() {
        MetricService metricService = mock(MetricService.class);
        
        StartWriterTask task = new StartWriterTask(metricService);
        task.execute(ImmutableMultimap.<String, String>of(), null);
        
        verify(metricService, times(1)).startWriter();
    }
}
