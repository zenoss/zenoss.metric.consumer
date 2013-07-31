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

import org.junit.Test;
import org.springframework.context.ApplicationContext;

import org.zenoss.app.consumer.metric.TsdbWriter;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
/**
 *
 * @author cschellenger
 */
public class OpenTsdbWriterFactoryTest {
    
    @Test
    public void testLifecycle() throws Exception {
        ApplicationContext appCtx = mock(ApplicationContext.class);
        TsdbWriter writer1 = mock(TsdbWriter.class);
        TsdbWriter writer2 = mock(TsdbWriter.class);
        
        when(appCtx.getBean(TsdbWriter.class)).thenReturn(writer1, writer2);
        
        OpenTsdbWriterFactory factory = new OpenTsdbWriterFactory(appCtx);
        assertEquals(writer1, factory.createWriter());
        assertEquals(writer2, factory.createWriter());
        factory.stop();
        
        verify(writer1, times(1)).cancel();
        verify(writer2, times(1)).cancel();
    }
}
